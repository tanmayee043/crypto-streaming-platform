"""Daily partitions, retention and the partitioning migration on a real Postgres (needs Docker)."""

from datetime import date, datetime, timezone
from pathlib import Path

import psycopg2
import pytest

import retention
from storage_consumer import to_row, write_batch
from tests.conftest import make_record
from tests.integration.conftest import count_rows

pytestmark = pytest.mark.integration

MIGRATION = (Path(__file__).resolve().parents[2] / "migrations" / "003_partition_trades_by_day.sql").read_text()

# the schema as it was before partitioning (schema.sql + migrations 001 and 002)
OLD_TRADES = """
    CREATE TABLE trades (
        id BIGSERIAL PRIMARY KEY, symbol TEXT NOT NULL, trade_id BIGINT NOT NULL,
        trade_time TIMESTAMPTZ NOT NULL, price NUMERIC(20, 8) NOT NULL, quantity NUMERIC(20, 8) NOT NULL,
        price_z DOUBLE PRECISION, volume_z DOUBLE PRECISION, size_ratio DOUBLE PRECISION,
        is_anomaly BOOLEAN NOT NULL DEFAULT FALSE, anomaly_type TEXT, received_at TIMESTAMPTZ,
        stored_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (symbol, trade_id))
"""


def ms(year, month, day, hour=12):
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp() * 1000)


def partition_of(conn, trade_id):
    with conn.cursor() as cur:
        cur.execute("SELECT tableoid::regclass::text FROM trades WHERE trade_id = %s", (trade_id,))
        return cur.fetchone()[0]


def test_trades_go_to_their_own_day_partition(conn):
    created = retention.ensure_partitions(conn, date(2026, 11, 2))
    assert created == [f"trades_202611{d:02d}" for d in range(2, 6)]
    write_batch(conn, [to_row(make_record(1, trade_time=ms(2026, 11, 3)))])
    assert partition_of(conn, 1) == "trades_20261103"
    assert retention.ensure_partitions(conn, date(2026, 11, 2)) == []  # already there


def test_redelivery_is_still_absorbed_across_partitions(conn):
    retention.ensure_partitions(conn, date(2026, 11, 2))
    rows = [to_row(make_record(i, trade_time=ms(2026, 11, 2 + i % 2))) for i in range(1, 11)]
    assert write_batch(conn, rows) == (10, 0)
    assert write_batch(conn, rows) == (0, 10)
    assert count_rows(conn) == 10


def test_old_partitions_are_dropped_and_recent_ones_kept(conn):
    retention.ensure_partitions(conn, date(2026, 8, 1), days_ahead=0)
    retention.ensure_partitions(conn, date(2026, 11, 1), days_ahead=0)
    write_batch(conn, [to_row(make_record(1, trade_time=ms(2026, 8, 1))),
                       to_row(make_record(2, trade_time=ms(2026, 11, 1)))])
    dropped = retention.drop_old_partitions(conn, today=date(2026, 11, 2), keep_days=30)
    assert dropped == ["trades_20260801"]
    with conn.cursor() as cur:
        cur.execute("SELECT trade_id FROM trades")
        assert cur.fetchall() == [(2,)]


def test_old_rows_in_the_default_partition_are_deleted(conn):
    write_batch(conn, [to_row(make_record(1, trade_time=ms(2026, 1, 1))),
                       to_row(make_record(2, trade_time=ms(2026, 11, 1)))])
    assert retention.delete_old_default(conn, today=date(2026, 11, 2), keep_days=30) == 1
    assert count_rows(conn) == 1


def test_a_day_already_in_the_default_partition_does_not_stop_the_run(conn):
    write_batch(conn, [to_row(make_record(1, trade_time=ms(2026, 11, 2)))])  # lands in default
    assert retention.ensure_partitions(conn, date(2026, 11, 2), days_ahead=1) == ["trades_20261103"]
    assert partition_of(conn, 1) == "trades_default"


def test_run_once_reports_what_it_did(conn):
    created, dropped = retention.run_once(conn, today=date(2026, 11, 2))
    assert len(created) == retention.DAYS_AHEAD + 1
    assert dropped == []


def test_migration_keeps_every_row_and_is_safe_to_rerun(conn):
    with conn, conn.cursor() as cur:
        cur.execute("DROP TABLE trades")
        cur.execute(OLD_TRADES)
        for i, day in enumerate([1, 1, 2, 3], start=1):
            cur.execute("INSERT INTO trades (symbol, trade_id, trade_time, price, quantity) "
                        "VALUES ('BTCUSDT', %s, %s, 65000.01, 0.5)",
                        (i, datetime(2026, 9, day, 12, tzinfo=timezone.utc)))
    with conn, conn.cursor() as cur:
        cur.execute(MIGRATION)
    with conn, conn.cursor() as cur:
        cur.execute(MIGRATION)  # second run does nothing

    assert count_rows(conn) == 4
    assert partition_of(conn, 1) == "trades_20260901"
    assert partition_of(conn, 4) == "trades_20260903"
    # trade 4 was migrated, so writing it again is an update; trade 5 is new and continues the ids
    assert write_batch(conn, [to_row(make_record(4, trade_time=ms(2026, 9, 3))),
                              to_row(make_record(5, trade_time=ms(2026, 9, 3)))]) == (1, 1)
    with conn.cursor() as cur:
        cur.execute("SELECT max(id) FROM trades")
        assert cur.fetchone()[0] >= 5
    with pytest.raises(psycopg2.errors.UndefinedTable):
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM trades_unpartitioned")
    conn.rollback()
