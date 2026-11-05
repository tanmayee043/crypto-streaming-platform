import json
import math
import statistics

import pytest

import anomaly_detector as ad
from tests.conftest import FakeMessage, FakeProducer, make_trade


@pytest.fixture(autouse=True)
def small_window(monkeypatch):
    """Use a 5-trade window and fresh per-symbol state in every test."""
    monkeypatch.setattr(ad, "WINDOW_SIZE", 5)
    monkeypatch.setattr(ad, "windows", {})
    monkeypatch.setattr(ad, "partition_of", {})
    monkeypatch.setattr(ad, "delivery_errors", [])


# --- robust z-score ---


def test_robust_z_of_constant_window_is_zero():
    assert ad.robust_z(5.0, [5.0] * 10) == 0.0


def test_one_outlier_in_the_window_hides_the_next_move_less_than_with_mean_and_stdev():
    # a realistic 100-value window, then the same window with one 20-sigma outlier in it
    clean = [statistics.NormalDist().inv_cdf((i + 0.5) / 100) for i in range(100)]
    polluted = clean[:-1] + [20.0]

    def classic_z(value, values):
        return (value - statistics.mean(values)) / statistics.pstdev(values)

    robust_kept = ad.robust_z(4.0, polluted) / ad.robust_z(4.0, clean)
    classic_kept = classic_z(4.0, polluted) / classic_z(4.0, clean)
    assert robust_kept > 0.8  # the 4-sigma move still scores at least 80% of its clean score
    assert classic_kept < 0.6  # the old mean/stdev score loses over 40% of it


def test_scale_floor_when_most_of_the_window_is_identical():
    # MAD is 0 here (most values equal), but the score must stay finite and sensible
    window = [0.0] * 7 + [0.001, -0.001, 0.002]
    z = ad.robust_z(0.001, window)
    assert math.isfinite(z)
    assert 0 < z < 10


# --- scoring a trade ---


def feed(window, prices, quantity=1.0):
    return [ad.score_trade(window, p, quantity) for p in prices]


def test_no_scores_until_the_window_is_full():
    window = ad.new_window()
    results = feed(window, [100.0, 100.1, 100.0, 100.1, 100.0])
    assert all(price_z is None for price_z, _ in results)  # 5 prices -> only 4 returns so far
    assert results[-1][1] is None  # size needs 5 EARLIER trades


def test_current_trade_is_not_in_its_own_baseline():
    window = ad.new_window()
    feed(window, [100.0, 100.1, 100.0, 100.1, 100.0, 100.1])
    returns_before = list(window["returns"])
    price_z, _ = ad.score_trade(window, 110.0, 1.0)  # a 10% jump
    assert price_z > ad.PRICE_Z_THRESHOLD
    assert list(window["returns"])[:-1] == returns_before[1:]  # added only after scoring


def test_log_returns_make_the_score_independent_of_the_price_level():
    cheap, expensive = ad.new_window(), ad.new_window()
    pattern = [1.0, 1.001, 1.0, 1.002, 1.001, 1.0, 1.05]
    z_cheap = feed(cheap, [p * 10 for p in pattern])[-1][0]
    z_expensive = feed(expensive, [p * 60_000 for p in pattern])[-1][0]
    assert z_cheap == pytest.approx(z_expensive)


def test_size_ratio_is_size_over_median_of_previous_trades():
    window = ad.new_window()
    for q in [1.0, 2.0, 2.0, 3.0, 100.0]:
        ad.score_trade(window, 100.0, q)
    _, size_ratio = ad.score_trade(window, 100.0, 20.0)
    assert size_ratio == pytest.approx(10.0)  # median of [1, 2, 2, 3, 100] is 2


@pytest.mark.parametrize("price, quantity", [(0.0, 1.0), (100.0, 0.0), (-1.0, 1.0)])
def test_rejects_impossible_trades(price, quantity):
    with pytest.raises(ValueError):
        ad.score_trade(ad.new_window(), price, quantity)


@pytest.mark.parametrize(
    ("price_z", "size_ratio", "expected"),
    [
        (None, None, None),  # warming up
        (6.0, 1.0, None),  # the threshold itself is not an anomaly
        (6.1, 1.0, "price"),
        (-6.1, None, "price"),
        (0.5, 50.0, "volume"),
        (None, 80.0, "volume"),
        (-9.0, 60.0, "both"),
    ],
)
def test_classify_anomaly(price_z, size_ratio, expected):
    assert ad.classify_anomaly(price_z, size_ratio) == expected


def test_each_symbol_has_its_own_window():
    for i in range(6):
        ad.update_window(make_trade(i, "BTCUSDT", price="65000"), partition=0)
    ad.update_window(make_trade(1, "SOLUSDT", price="150"), partition=2)
    assert len(ad.windows["BTCUSDT"]["sizes"]) == 5
    assert len(ad.windows["SOLUSDT"]["sizes"]) == 1
    assert ad.partition_of == {"BTCUSDT": 0, "SOLUSDT": 2}


def test_revoking_a_partition_drops_only_its_symbols():
    ad.update_window(make_trade(1, "BTCUSDT"), partition=0)
    ad.update_window(make_trade(1, "SOLUSDT"), partition=2)

    class TP:
        partition = 0

    ad.on_revoke(None, [TP()])
    assert set(ad.windows) == {"SOLUSDT"}


# --- process_batch: publish first, commit second ---


class FakeConsumer:
    def __init__(self):
        self.commits = []

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.commits.append([(o.partition, o.offset) for o in offsets])


def raw_message(trade, offset, partition=0):
    return FakeMessage(json.dumps(trade).encode(), offset, partition, topic="crypto-trades", key=trade["s"].encode())


def test_results_are_flushed_before_offsets_are_committed():
    consumer, producer = FakeConsumer(), FakeProducer()
    ad.process_batch(consumer, producer, [raw_message(make_trade(i), i) for i in range(3)])
    assert producer.flushes == 1
    assert consumer.commits == [[(0, 3)]]


def test_results_are_keyed_by_symbol_and_warm_up_trades_are_still_published():
    consumer, producer = FakeConsumer(), FakeProducer()
    ad.process_batch(consumer, producer, [raw_message(make_trade(1, "ETHUSDT"), 0)])
    [sent] = producer.sent
    result = json.loads(sent["value"])
    assert sent["topic"] == ad.OUTPUT_TOPIC
    assert sent["key"] == b"ETHUSDT"
    assert (result["trade_id"], result["price_z"], result["is_anomaly"]) == (1, None, False)


def test_nothing_is_committed_if_kafka_did_not_take_the_results():
    consumer, producer = FakeConsumer(), FakeProducer(undelivered_on_flush=2)
    with pytest.raises(RuntimeError):
        ad.process_batch(consumer, producer, [raw_message(make_trade(1), 0)])
    assert consumer.commits == []


def test_bad_trade_goes_to_dead_letter_topic_and_offset_advances():
    consumer, producer = FakeConsumer(), FakeProducer()
    bad = FakeMessage(b'{"s": "BTCUSDT", "p": "0", "q": "1"}', 0, topic="crypto-trades")
    ad.process_batch(consumer, producer, [bad, raw_message(make_trade(2), 1)])
    assert [m["topic"] for m in producer.sent] == [ad.DEAD_LETTER_TOPIC, ad.OUTPUT_TOPIC]
    assert consumer.commits == [[(0, 2)]]


def test_detector_has_auto_commit_off_and_reads_from_the_start():
    config = ad.consumer_config()
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"
