import psycopg2
from prometheus_client import Gauge, start_http_server

import argparse
import logging
import re
import time
from datetime import datetime, timedelta, timezone

import os
from dotenv import load_dotenv
load_dotenv()

from service_setup import exit_on_sigterm, postgres_args, settings, setup_logging  # noqa: E402

setup_logging()
log = logging.getLogger("retention")

# Keeps the database from filling the disk. trades is partitioned by day, so this service:
# - creates the next few days' partitions ahead of time,
# - drops whole partitions older than RETENTION_DAYS (instant, and frees the disk space),
# - deletes old rows from the catch-all default partition.

RETENTION_DAYS = settings.retention_days
DAYS_AHEAD = 3
CHECK_INTERVAL_SECONDS = settings.retention_interval_seconds
METRICS_PORT = int(os.getenv("METRICS_PORT", "8005"))  # 0 turns the /metrics endpoint off
METRICS_HOST = os.getenv("METRICS_HOST", "127.0.0.1")

DATABASE_BYTES = Gauge("pipeline_database_size_bytes", "Size of the Postgres database")
PARTITIONS = Gauge("pipeline_trade_partitions", "Daily partitions of the trades table")

PARTITION_NAME = re.compile(r"^trades_(\d{8})$")


def connect_postgres():
    return psycopg2.connect(**postgres_args())


def utc_midnight(day):
    return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)


def daily_partitions(conn):
    """{date: partition name} for the existing daily partitions of trades."""
    with conn.cursor() as cur:
        cur.execute("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'trades'::regclass")
        names = [row[0] for row in cur.fetchall()]
    found = {}
    for name in names:
        match = PARTITION_NAME.match(name)
        if match:
            found[datetime.strptime(match.group(1), "%Y%m%d").date()] = name
    return found


def ensure_partitions(conn, today, days_ahead=DAYS_AHEAD):
    """Create partitions for today and the next few days. Returns the names created."""
    existing = daily_partitions(conn)
    created = []
    for offset in range(days_ahead + 1):
        day = today + timedelta(days=offset)
        if day in existing:
            continue
        name = f"trades_{day:%Y%m%d}"
        try:
            with conn, conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {name} PARTITION OF trades FOR VALUES FROM (%s) TO (%s)",
                            (utc_midnight(day), utc_midnight(day + timedelta(days=1))))
            created.append(name)
        except psycopg2.errors.CheckViolation:
            # That day's rows are already in the default partition (the service was down over
            # midnight). They stay there, are still queryable, and get pruned by delete_old_default.
            log.warning(f"Could not create {name}: rows for that day are in the default partition")
    return created


def drop_old_partitions(conn, today, keep_days=RETENTION_DAYS):
    """Drop whole daily partitions older than keep_days. Returns the names dropped."""
    cutoff = today - timedelta(days=keep_days)
    dropped = []
    for day, name in sorted(daily_partitions(conn).items()):
        if day < cutoff:
            with conn, conn.cursor() as cur:
                cur.execute(f"DROP TABLE {name}")
            dropped.append(name)
    return dropped


def delete_old_default(conn, today, keep_days=RETENTION_DAYS):
    """Rows that landed in the default partition, plus resolved gap records, age out here."""
    cutoff = utc_midnight(today - timedelta(days=keep_days))
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM trades_default WHERE trade_time < %s", (cutoff,))
        deleted = cur.rowcount
        cur.execute("DELETE FROM trade_gaps WHERE detected_at < %s", (cutoff,))
    return deleted


def run_once(conn, today=None):
    today = today or datetime.now(timezone.utc).date()
    created = ensure_partitions(conn, today)
    dropped = drop_old_partitions(conn, today)
    deleted = delete_old_default(conn, today)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_database_size(current_database())")
        size = cur.fetchone()[0]
    conn.rollback()
    DATABASE_BYTES.set(size)
    PARTITIONS.set(len(daily_partitions(conn)))
    conn.rollback()
    log.info(f"Partitions: created {created or 'none'}, dropped {dropped or 'none'}; "
             f"{deleted} old default rows deleted; database is {size / 1_000_000:.0f} MB "
             f"(keeping {RETENTION_DAYS} days)")
    return created, dropped


def main():
    parser = argparse.ArgumentParser(description="Create and prune the daily partitions of trades")
    parser.add_argument("--once", action="store_true", help="run once and exit")
    args = parser.parse_args()

    exit_on_sigterm()
    if METRICS_PORT and not args.once:
        start_http_server(METRICS_PORT, addr=METRICS_HOST)
    conn = None
    try:
        while True:
            try:
                if conn is None or conn.closed:
                    conn = connect_postgres()
                run_once(conn)
            except psycopg2.Error as e:  # e.g. Postgres restarting: try again next round
                log.error(f"Retention run failed: {e}")
                if conn is not None:
                    conn.close()
            if args.once:
                break
            time.sleep(CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
