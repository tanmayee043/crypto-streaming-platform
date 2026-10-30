"""gap_checker.py against a real Postgres (needs Docker)."""

import pytest

import gap_checker
from storage_consumer import to_row, write_batch
from tests.conftest import make_record

pytestmark = pytest.mark.integration

ALL_TIME = 24 * 365 * 100  # hours: the test trades are from 2023


def store(conn, symbol, ids):
    write_batch(conn, [to_row(make_record(i, symbol=symbol)) for i in ids])


def test_no_gaps_means_100_percent(conn):
    store(conn, "BTCUSDT", range(1, 11))
    [report] = gap_checker.completeness(conn, ALL_TIME)
    assert (report["stored"], report["expected"], report["completeness_pct"]) == (10, 10, 100.0)
    assert gap_checker.find_gaps(conn, ALL_TIME) == []


def test_finds_each_missing_range_per_symbol(conn):
    store(conn, "BTCUSDT", [1, 2, 3, 6, 7, 9, 10])   # 4-5 and 8 missing
    store(conn, "ETHUSDT", [100, 101, 102])
    gaps = gap_checker.find_gaps(conn, ALL_TIME)
    assert gaps == [("BTCUSDT", 4, 5), ("BTCUSDT", 8, 8)]
    btc = next(r for r in gap_checker.completeness(conn, ALL_TIME) if r["symbol"] == "BTCUSDT")
    assert (btc["stored"], btc["expected"], btc["completeness_pct"]) == (7, 10, 70.0)


def test_gaps_are_recorded_once_and_resolved_when_filled(conn):
    store(conn, "BTCUSDT", [1, 2, 5])
    gap_checker.record_gaps(conn, gap_checker.find_gaps(conn, ALL_TIME))
    gap_checker.record_gaps(conn, gap_checker.find_gaps(conn, ALL_TIME))  # second run: no duplicate
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, first_missing_id, last_missing_id, resolved_at FROM trade_gaps")
        assert cur.fetchall() == [("BTCUSDT", 3, 4, None)]

    store(conn, "BTCUSDT", [3, 4])  # e.g. a backfill
    resolved = gap_checker.record_gaps(conn, gap_checker.find_gaps(conn, ALL_TIME))
    assert resolved == 1
    with conn.cursor() as cur:
        cur.execute("SELECT resolved_at IS NOT NULL FROM trade_gaps")
        assert cur.fetchone()[0] is True
