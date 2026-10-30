import psycopg2
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Gauge, start_http_server

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

CHECK_INTERVAL_SECONDS = float(os.getenv("GAP_CHECK_INTERVAL_SECONDS", "15"))
LOOKBACK_HOURS = float(os.getenv("GAP_CHECK_LOOKBACK_HOURS", "24"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8004"))  # 0 turns the /metrics endpoint off
# localhost only: Prometheus runs on this machine, and nothing else should reach the metrics
METRICS_HOST = os.getenv("METRICS_HOST", "127.0.0.1")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# (consumer group, topic it reads) for the two consumers whose lag we watch
WATCHED_GROUPS = [
    (os.getenv("DETECTOR_GROUP_ID", "crypto-anomaly-detector"), os.getenv("TRADES_TOPIC", "crypto-trades")),
    (os.getenv("STORAGE_GROUP_ID", "storage-writer"), os.getenv("ANALYTICS_TOPIC", "trade-analytics")),
]

# --- Prometheus metrics (scraped from http://localhost:8004/metrics) ---
COMPLETENESS = Gauge("pipeline_completeness_ratio", "Share of trade IDs stored over the lookback window", ["symbol"])
MISSING = Gauge("pipeline_missing_trades", "Trade IDs missing over the lookback window", ["symbol"])
FRESHNESS = Gauge("pipeline_seconds_since_last_trade", "Seconds since the newest stored trade", ["symbol"])
CONSUMER_LAG = Gauge("pipeline_consumer_lag_messages", "Messages a consumer group has not processed yet",
                     ["group", "topic"])

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


FRESHNESS_SQL = "SELECT symbol, max(stored_at) FROM trades WHERE stored_at IS NOT NULL GROUP BY symbol"


def freshness(conn):
    """Seconds since each symbol's newest stored trade. Both times are this machine's clock."""
    with conn.cursor() as cur:
        cur.execute(FRESHNESS_SQL)
        rows = cur.fetchall()
    now = time.time()
    return {symbol: round(now - stored_at.timestamp(), 1) for symbol, stored_at in rows}


def consumer_lag(groups=None):
    """{(group, topic): messages not yet processed}. Missing topics are skipped."""
    lag = {}
    for group, topic in groups or WATCHED_GROUPS:
        consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "group.id": group,
                             "enable.auto.commit": False})
        try:
            metadata = consumer.list_topics(topic, timeout=5).topics.get(topic)
            if metadata is None or metadata.error is not None:
                continue
            partitions = [TopicPartition(topic, p) for p in metadata.partitions]
            total = 0
            for tp in consumer.committed(partitions, timeout=5):
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                position = tp.offset if tp.offset >= 0 else low  # never committed: starts at the beginning
                total += max(high - position, 0)
            lag[(group, topic)] = total
        finally:
            consumer.close()
    return lag


def check(conn, hours):
    report = completeness(conn, hours)
    gaps = find_gaps(conn, hours)
    resolved = record_gaps(conn, gaps)
    for r in report:
        missing = r["expected"] - r["stored"]
        open_gaps = sum(1 for g in gaps if g[0] == r["symbol"])
        COMPLETENESS.labels(r["symbol"]).set(r["stored"] / r["expected"])
        MISSING.labels(r["symbol"]).set(missing)
        # only log when something is missing, so a healthy pipeline doesn't flood the log
        level = logging.WARNING if missing else logging.DEBUG
        log.log(level, f"{r['symbol']}: {r['completeness_pct']:.2f}% complete over {hours:g}h "
                       f"({r['stored']}/{r['expected']} trades, {missing} missing in {open_gaps} gaps)")
    for symbol, seconds in freshness(conn).items():
        FRESHNESS.labels(symbol).set(seconds)
    try:
        for (group, topic), messages in consumer_lag().items():
            CONSUMER_LAG.labels(group, topic).set(messages)
    except Exception as e:  # Kafka down shouldn't stop the database checks
        log.warning(f"Could not read consumer lag: {e}")
    if resolved:
        log.info(f"{resolved} earlier gaps are now filled in")
    return report, gaps


def main():
    parser = argparse.ArgumentParser(description="Report missing Binance trade IDs and completeness")
    parser.add_argument("--once", action="store_true", help="check once and exit")
    parser.add_argument("--hours", type=float, default=LOOKBACK_HOURS, help="how far back to check")
    args = parser.parse_args()

    conn = connect_postgres()
    if METRICS_PORT and not args.once:
        start_http_server(METRICS_PORT, addr=METRICS_HOST)
    try:
        while True:
            report, gaps = check(conn, args.hours)
            if args.once:
                for r in report:
                    log.info(f"{r['symbol']}: {r['completeness_pct']:.2f}% complete "
                             f"({r['stored']}/{r['expected']} trades)")
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
