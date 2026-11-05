import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import alerts


def anomaly(trade_id, kind="price"):
    return {
        "symbol": "BTCUSDT",
        "trade_id": trade_id,
        "price": 65000.5,
        "anomaly_type": kind,
        "price_z": 7.2,
        "size_ratio": 1.5,
    }


def healthy(**overrides):
    health = {
        "completeness": [{"symbol": "BTCUSDT", "completeness_pct": 100.0}],
        "latency_ms": {"p95": 120.0},
        "consumer_lag": [{"group": "storage-writer", "messages": 3}],
    }
    health.update(overrides)
    return health


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def sent():
    return []


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def alerter(sent, clock):
    return alerts.Alerter(sender=sent.append, clock=clock)


def test_first_anomaly_is_sent_at_once(alerter, sent):
    alerter.anomalies([anomaly(1)])
    assert len(sent) == 1
    assert "PRICE BTCUSDT #1" in sent[0]


def test_anomalies_within_the_interval_are_summarised_in_one_message(alerter, sent, clock):
    alerter.anomalies([anomaly(1)])
    for i in range(2, 9):
        alerter.anomalies([anomaly(i, "volume")])
    assert len(sent) == 1  # still inside the interval
    clock.now += alerts.ANOMALY_ALERT_INTERVAL_SECONDS
    alerter.anomalies([])
    assert len(sent) == 2
    assert sent[1].startswith("🚨 7 anomalies")
    assert "...and 2 more" in sent[1]


def test_slo_breach_is_reported_once_and_recovery_once(alerter, sent):
    slow = healthy(latency_ms={"p95": 3500.0})
    alerter.health(slow)
    alerter.health(slow)
    assert sent == ["⚠️ SLO breach: p95 latency 3.5s (target 2s)"]
    alerter.health(healthy())
    assert sent[-1] == "✅ Recovered: latency is back within its target"
    alerter.health(healthy())
    assert len(sent) == 2


def test_each_slo_is_checked():
    breaches = alerts.slo_breaches(
        healthy(
            completeness=[{"symbol": "ETHUSDT", "completeness_pct": 99.0}],
            consumer_lag=[{"group": "crypto-anomaly-detector", "messages": 5000}],
        )
    )
    assert set(breaches) == {"completeness:ETHUSDT", "lag:crypto-anomaly-detector"}
    assert alerts.slo_breaches(healthy()) == {}


def test_without_a_webhook_alerts_are_only_logged(monkeypatch):
    monkeypatch.setattr(alerts, "DISCORD_WEBHOOK_URL", "")
    assert alerts.send("hello") is False


def test_send_posts_discord_json_to_the_webhook(monkeypatch):
    received = []

    class Hook(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.handle_request, daemon=True).start()
    monkeypatch.setattr(alerts, "DISCORD_WEBHOOK_URL", f"http://127.0.0.1:{server.server_port}/hook")
    assert alerts.send("🚨 test") is True
    server.server_close()
    assert received == [{"content": "🚨 test"}]


def test_a_broken_webhook_never_raises(monkeypatch):
    monkeypatch.setattr(alerts, "DISCORD_WEBHOOK_URL", "http://127.0.0.1:9/nothing-listens-here")
    assert alerts.send("hello") is False
