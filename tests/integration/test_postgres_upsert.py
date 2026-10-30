"""Runs the real upsert against a throwaway Postgres container (needs Docker)."""

from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from storage_consumer import to_row, write_batch, write_with_retry
from tests.conftest import make_record
from tests.integration.conftest import count_rows

pytestmark = pytest.mark.integration

def test_writing_the_same_batch_twice_leaves_one_copy(conn):
    rows = [to_row(make_record(i)) for i in range(1, 101)]
    assert write_batch(conn, rows) == (100, 0)
    assert write_batch(conn, rows) == (0, 100)  # the redelivery is absorbed as updates
    assert count_rows(conn) == 100


def test_redelivery_updates_values(conn):
    write_batch(conn, [to_row(make_record(1, price_z=0.1))])
    write_batch(conn, [to_row(make_record(1, price_z=4.2, is_anomaly=True, anomaly_type="price"))])
    with conn.cursor() as cur:
        cur.execute("SELECT price_z, is_anomaly, anomaly_type FROM trades")
        assert cur.fetchall() == [(4.2, True, "price")]


def test_same_trade_id_on_different_symbols_are_different_rows(conn):
    write_batch(conn, [to_row(make_record(1)), to_row(make_record(1, symbol="ETHUSDT"))])
    assert count_rows(conn) == 2


def test_failed_batch_writes_nothing(conn):
    good = to_row(make_record(1))
    # trade_time is NOT NULL, so this second row makes the statement fail mid-batch
    broken = (*to_row(make_record(2))[:2], None, *to_row(make_record(2))[3:])
    with pytest.raises(psycopg2.Error):
        write_batch(conn, [good, broken])
    assert count_rows(conn) == 0


def test_retry_reconnects_after_connection_loss(postgres_url, conn):
    broken = psycopg2.connect(postgres_url)
    broken.close()  # simulate Postgres dropping the connection
    new_conn, inserted, _ = write_with_retry(
        broken,
        [to_row(make_record(1))],
        reconnect=lambda: psycopg2.connect(postgres_url),
        sleep=lambda _s: None,
    )
    assert inserted == 1
    assert count_rows(conn) == 1
    new_conn.close()


def test_prices_are_stored_exactly(conn):
    write_batch(conn, [to_row(make_record(1, price=65000.01, quantity=0.00012345))])
    with conn.cursor() as cur:
        cur.execute("SELECT price::text, quantity::text FROM trades")
        assert cur.fetchone() == ("65000.01000000", "0.00012345")


def test_redelivery_keeps_the_first_stored_at(conn):
    first = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
    write_batch(conn, [to_row(make_record(1), first)])
    write_batch(conn, [to_row(make_record(1), first + timedelta(minutes=5))])
    with conn.cursor() as cur:
        cur.execute("SELECT stored_at FROM trades")
        assert cur.fetchone()[0] == first
