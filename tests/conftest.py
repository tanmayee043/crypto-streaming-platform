import sys
from pathlib import Path

# the services are plain scripts in the repo root; make them importable from tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class FakeMessage:
    """Stands in for confluent_kafka.Message in unit tests."""

    def __init__(self, value, offset, partition=0, topic="trade-analytics", error=None):
        self._value = value
        self._offset = offset
        self._partition = partition
        self._topic = topic
        self._error = error

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return self._topic

    def error(self):
        return self._error


def make_record(trade_id, symbol="BTCUSDT", **overrides):
    """A trade-analytics message, as anomaly_detector.py publishes it."""
    record = {
        "symbol": symbol,
        "trade_id": trade_id,
        "trade_time": 1_700_000_000_000 + trade_id,
        "received_at": 1_700_000_000_500.0 + trade_id,
        "price": 50_000.0 + trade_id,
        "quantity": 0.01,
        "price_z": 0.5,
        "volume_z": -0.2,
        "is_anomaly": False,
        "anomaly_type": None,
    }
    record.update(overrides)
    return record
