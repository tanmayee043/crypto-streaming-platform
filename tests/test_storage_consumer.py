"""The delivery guarantee, tested with fakes: offsets are committed only after a good write."""

import json
from datetime import datetime, timezone

import psycopg2
import pytest
from confluent_kafka import KafkaException

import storage_consumer as sc
from tests.conftest import FakeMessage, FakeProducer, make_record


class FakeConsumer:
    def __init__(self, events):
        self.events = events
        self.commits = []
        self.fail_commit = False

    def commit(self, offsets, asynchronous):
        assert asynchronous is False, "commits must be synchronous so we know they happened"
        self.events.append("commit")
        if self.fail_commit:
            raise KafkaException("rebalance in progress")
        self.commits.append([(o.partition, o.offset) for o in offsets])


class FakeConn:
    def close(self):
        pass


@pytest.fixture
def events():
    return []


@pytest.fixture
def written(monkeypatch, events):
    """Replaces the real Postgres write; set `fail_next` to make the next N writes fail."""
    state: dict = {"calls": [], "fail_next": 0, "raise": psycopg2.OperationalError}

    def fake_write_batch(conn, rows):
        state["calls"].append(list(rows))
        if state["fail_next"]:
            state["fail_next"] -= 1
            raise state["raise"]("server closed the connection unexpectedly")
        events.append("write")
        return len(rows), 0

    monkeypatch.setattr(sc, "write_batch", fake_write_batch)
    monkeypatch.setattr(sc, "connect_postgres", FakeConn)
    monkeypatch.setattr(sc.time, "sleep", lambda _s: None)
    return state


def message(trade_id, offset, partition=0):
    return FakeMessage(json.dumps(make_record(trade_id)).encode(), offset, partition)


# --- consumer settings: regression test for the original bug ---


def test_auto_commit_is_off():
    config = sc.consumer_config()
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False


# --- helpers ---


def test_to_row_converts_epoch_ms_to_utc():
    row = sc.to_row(make_record(7))
    assert row[:2] == ("BTCUSDT", 7)
    assert row[2] == datetime(2023, 11, 14, 22, 13, 20, 7000, tzinfo=timezone.utc)


def test_to_row_rejects_missing_field():
    record = make_record(1)
    del record["trade_id"]
    with pytest.raises(KeyError):
        sc.to_row(record)


def test_dedupe_keeps_last_copy_per_symbol_and_trade_id():
    rows = [
        sc.to_row(make_record(1, price_z=0.1)),
        sc.to_row(make_record(2)),
        sc.to_row(make_record(1, price_z=0.9)),
        sc.to_row(make_record(1, symbol="ETHUSDT")),
    ]
    deduped = sc.dedupe_rows(rows)
    assert len(deduped) == 3
    assert next(r for r in deduped if r[:2] == ("BTCUSDT", 1))[5] == 0.9


def test_next_offsets_is_highest_offset_plus_one_per_partition():
    messages = [FakeMessage(b"", 10), FakeMessage(b"", 12), FakeMessage(b"", 11), FakeMessage(b"", 3, partition=2)]
    assert [(o.partition, o.offset) for o in sc.next_offsets(messages)] == [(0, 13), (2, 4)]


# --- process_batch: write first, commit second ---


def test_commits_offsets_after_successful_write(written, events):
    consumer = FakeConsumer(events)
    batch = [message(1, 0), message(2, 1), message(3, 0, partition=1)]
    sc.process_batch(consumer, FakeConn(), batch, FakeProducer())
    assert events == ["write", "commit"]
    assert consumer.commits == [[(0, 2), (1, 1)]]


def test_retries_the_same_batch_until_postgres_recovers(written, events):
    written["fail_next"] = 3
    consumer = FakeConsumer(events)
    sc.process_batch(consumer, FakeConn(), [message(1, 0)], FakeProducer())
    assert len(written["calls"]) == 4
    assert all(call == written["calls"][0] for call in written["calls"])
    assert events == ["write", "commit"]


def test_crash_during_write_commits_nothing(written, events):
    # KeyboardInterrupt stands in for the process dying mid-write
    written["fail_next"] = 1
    written["raise"] = KeyboardInterrupt
    consumer = FakeConsumer(events)
    with pytest.raises(KeyboardInterrupt):
        sc.process_batch(consumer, FakeConn(), [message(1, 0)], FakeProducer())
    assert consumer.commits == []


def test_malformed_message_goes_to_dead_letter_topic_and_offset_advances(written, events):
    consumer = FakeConsumer(events)
    producer = FakeProducer()
    batch = [message(1, 0), FakeMessage(b"{not json", 1), message(2, 2)]
    sc.process_batch(consumer, FakeConn(), batch, producer)
    assert [r[1] for r in written["calls"][0]] == [1, 2]
    assert [m["topic"] for m in producer.sent] == ["trade-analytics-dlq"]
    assert producer.sent[0]["value"] == b"{not json"  # original bytes kept
    assert producer.sent[0]["headers"]["source_offset"] == "1"
    assert producer.flushes == 1  # dead letter confirmed before the commit
    assert consumer.commits == [[(0, 3)]]


def test_no_commit_if_dead_letter_cannot_be_delivered(written, events):
    consumer = FakeConsumer(events)
    with pytest.raises(RuntimeError):
        sc.process_batch(consumer, FakeConn(), [FakeMessage(b"{not json", 0)], FakeProducer(undelivered_on_flush=1))
    assert consumer.commits == []


def test_to_row_keeps_prices_exact_and_adds_timestamps():
    stored_at = datetime(2026, 10, 29, tzinfo=timezone.utc)
    row = sc.to_row(make_record(1, price=65000.01, quantity=0.00012, size_ratio=2.5), stored_at)
    assert str(row[3]) == "65000.01"
    assert str(row[4]) == "0.00012"
    assert row[9] == 2.5
    assert row[10] == datetime.fromtimestamp(1_700_000_000_501 / 1000, tz=timezone.utc)
    assert row[11] == stored_at


def test_duplicate_trade_in_one_batch_is_written_once(written, events):
    consumer = FakeConsumer(events)
    sc.process_batch(consumer, FakeConn(), [message(1, 0), message(1, 1)], FakeProducer())
    assert len(written["calls"][0]) == 1
    assert consumer.commits == [[(0, 2)]]


def test_kafka_error_events_are_not_written_or_committed(written, events):
    consumer = FakeConsumer(events)
    sc.process_batch(consumer, FakeConn(), [FakeMessage(None, -1, error="broker transport failure")], FakeProducer())
    assert written["calls"] == []
    assert consumer.commits == []


def test_failed_commit_is_logged_not_fatal(written, events):
    consumer = FakeConsumer(events)
    consumer.fail_commit = True
    sc.process_batch(consumer, FakeConn(), [message(1, 0)], FakeProducer())
    assert events == ["write", "commit"]


def test_consume_batch_does_not_wait_for_a_full_batch():
    from kafka_helpers import consume_batch

    calls = []

    class QuietConsumer:
        def consume(self, num_messages, timeout):
            calls.append((num_messages, timeout))
            return ["m1"] if num_messages == 1 else []  # one trade waiting, nothing else

    assert consume_batch(QuietConsumer(), 500, 1.0) == ["m1"]
    assert calls == [(1, 1.0), (499, 0)]  # second call never waits
