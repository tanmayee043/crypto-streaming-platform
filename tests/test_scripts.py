"""The pure helpers in the replay and recording scripts."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from record_trades import check_ids  # noqa: E402
from replay import send_times  # noqa: E402


def trade(symbol, trade_id, t=0):
    return {"s": symbol, "t": trade_id, "T": t}


def test_check_ids_counts_missing_duplicate_and_out_of_order_ids():
    trades = [trade("BTCUSDT", i) for i in (1, 2, 3, 5, 5, 4, 8)] + [trade("ETHUSDT", i) for i in (10, 11)]
    report = check_ids(trades)
    assert report["BTCUSDT"] == {
        "trades": 7,
        "first_id": 1,
        "last_id": 8,
        "missing": 2,
        "duplicates": 1,
        "out_of_order": 1,
    }
    assert report["ETHUSDT"]["missing"] == 0


def test_replay_keeps_original_gaps_scaled_by_speed():
    trades = [trade("BTCUSDT", 1, 0), trade("BTCUSDT", 2, 1000), trade("BTCUSDT", 3, 5000)]
    assert send_times(trades, speed=10) == pytest.approx([0.0, 0.1, 0.5])


def test_replay_at_a_fixed_rate_ignores_original_timing():
    trades = [trade("BTCUSDT", i, i * 99_999) for i in range(4)]
    assert send_times(trades, rate=2) == [0.0, 0.5, 1.0, 1.5]


def test_replay_speed_zero_sends_everything_at_once():
    assert send_times([trade("BTCUSDT", 1, 0), trade("BTCUSDT", 2, 9000)], speed=0) == [0.0, 0.0]
