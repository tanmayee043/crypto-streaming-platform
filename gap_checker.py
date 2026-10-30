import psycopg2

import argparse
import logging
import time

import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("gap_checker")

# Binance trade IDs are sequential per symbol (checked on 120,000 REST trades and a live
# WebSocket recording: no jumps). So for each symbol, every ID between the lowest and highest
# one we stored should be in the table. Any missing ID is a trade the pipeline lost, wherever
# it was lost (WebSocket, producer, detector or storage), because we check the end result.

CHECK_INTERVAL_SECONDS = float(os.getenv("GAP_CHECK_INTERVAL_SECONDS", "60"))
LOOKBACK_HOURS = float(os.getenv("GAP_CHECK_LOOKBACK_HOURS", "24"))

COMPLETENESS_SQL = """
    SELECT symbol,
           count(*) AS stored,
           max(trade_id) - min(trade_id) + 1 AS expected,
           min(trade_id) AS first_id,
           max(trade_id) AS last_id
    FROM trades
    WHERE trade_time >= now() - make_interval(secs => %s)
    GROUP BY symbol
    ORDER BY symbol
"""

# LAG() looks at the previous trade_id of the same symbol; a jump of more than 1 is a gap
GAPS_SQL = """
    SELECT symbol, prev_id + 1 AS first_missing_id, trade_id - 1 AS last_missing_id
    FROM (
        SELECT symbol, trade_id,
               LAG(trade_id) OVER (PARTITION BY symbol ORDER BY trade_id) AS prev_id
        FROM trades
        WHERE trade_time >= now() - make_interval(secs => %s)
    ) t
    WHERE trade_id > prev_id + 1
    ORDER BY symbol, first_missing_id
"""

RECORD_GAP_SQL = """
    INSERT INTO trade_gaps (symbol, first_missing_id, last_missing_id)
    VALUES (%s, %s, %s)
    ON CONFLICT (symbol, first_missing_id) DO UPDATE SET last_missing_id = EXCLUDED.last_missing_id
"""

# A gap is resolved once every trade in its range has been filled in (e.g. by a backfill)
RESOLVE_GAPS_SQL = """
    UPDATE trade_gaps g SET resolved_at = now()
    WHERE resolved_at IS NULL
      AND (SELECT count(*) FROM trades t
           WHERE t.symbol = g.symbol AND t.trade_id BETWEEN g.first_missing_id AND g.last_missing_id)
          = g.last_missing_id - g.first_missing_id + 1
"""


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def completeness(conn, hours):
    """Per symbol: stored, expected, first/last id and completeness % over the last `hours`."""
    with conn.cursor() as cur:
        cur.execute(COMPLETENESS_SQL, (hours * 3600,))
        rows = cur.fetchall()
    return [
        {"symbol": symbol, "stored": stored, "expected": expected, "first_id": first_id,
         "last_id": last_id, "completeness_pct": round(stored / expected * 100, 4)}
        for symbol, stored, expected, first_id, last_id in rows
    ]


def find_gaps(conn, hours):
    with conn.cursor() as cur:
        cur.execute(GAPS_SQL, (hours * 3600,))
        return cur.fetchall()


def record_gaps(conn, gaps):
    with conn, conn.cursor() as cur:
        for gap in gaps:
            cur.execute(RECORD_GAP_SQL, gap)
        cur.execute(RESOLVE_GAPS_SQL)
        return cur.rowcount  # gaps resolved this round


def check(conn, hours):
    report = completeness(conn, hours)
    gaps = find_gaps(conn, hours)
    resolved = record_gaps(conn, gaps)
    for r in report:
        missing = r["expected"] - r["stored"]
        open_gaps = sum(1 for g in gaps if g[0] == r["symbol"])
        log.info(f"{r['symbol']}: {r['completeness_pct']:.2f}% complete over {hours:g}h "
                 f"({r['stored']}/{r['expected']} trades, {missing} missing in {open_gaps} gaps)")
    if resolved:
        log.info(f"{resolved} earlier gaps are now filled in")
    return report, gaps


def main():
    parser = argparse.ArgumentParser(description="Report missing Binance trade IDs and completeness")
    parser.add_argument("--once", action="store_true", help="check once and exit")
    parser.add_argument("--hours", type=float, default=LOOKBACK_HOURS, help="how far back to check")
    args = parser.parse_args()

    conn = connect_postgres()
    try:
        while True:
            _, gaps = check(conn, args.hours)
            if args.once:
                for symbol, first, last in gaps[:20]:
                    log.info(f"  gap {symbol}: ids {first}-{last} ({last - first + 1} missing)")
                break
            time.sleep(CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
