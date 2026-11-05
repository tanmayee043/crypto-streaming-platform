"""API pieces that don't need a database: explanations, event format and replay mode."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client():
    # no `with`: the live-stream background loop (which needs Postgres) isn't started
    return TestClient(main.app)


def row(**overrides):
    base = {
        "symbol": "BTCUSDT",
        "trade_id": 10,
        "price": Decimal("65650.00"),
        "price_z": 7.5,
        "size_ratio": 2.0,
        "volume_z": None,
        "is_anomaly": True,
        "anomaly_type": "price",
    }
    base.update(overrides)
    return base


def test_explain_says_why_a_price_anomaly_was_flagged():
    reasons = main.explain(row(), previous_price=Decimal("65000.00"))
    price, size = reasons
    assert price["flagged"] is True
    assert "+1.000%" in price["text"] and "7.5 robust standard deviations" in price["text"]
    assert size["flagged"] is False and "2.0x the median" in size["text"]


def test_explain_for_a_warm_up_trade():
    [reason] = main.explain(row(price_z=None, size_ratio=None, is_anomaly=False), previous_price=None)
    assert reason["signal"] == "warm-up"


def test_explain_for_rows_scored_by_the_original_model():
    [reason] = main.explain(row(size_ratio=None, volume_z=4.0, price_z=0.1), previous_price=Decimal("1"))
    assert reason["signal"] == "v1 model"
    assert "volume z-score 4.00" in reason["text"]


def test_sse_event_format():
    text = main.sse("trades", [{"price": Decimal("1.5"), "t": datetime(2026, 1, 1, tzinfo=timezone.utc)}], 7)
    assert text.startswith("id: 7\nevent: trades\ndata: ")
    assert text.endswith("\n\n")
    assert json.loads(text.split("data: ", 1)[1]) == [{"price": 1.5, "t": "2026-01-01T00:00:00+00:00"}]


def test_replay_files_lists_the_committed_recordings(client):
    names = {f["name"] for f in client.get("/replay/files").json()}
    assert {"sample_trades.jsonl", "crash-2025-10-10.jsonl"} <= names


@pytest.mark.parametrize("name", ["../main.py", "..%2F..%2F.env", "missing.jsonl"])
def test_replay_only_serves_files_from_the_replay_folders(client, name):
    assert client.get(f"/replay/stream?file={name}").status_code == 404


def test_replay_streams_scored_trades_then_finishes(client, monkeypatch):
    monkeypatch.setattr(main, "load_trades", lambda path: _first_trades(path, 400))  # keep the test quick
    with client.stream("GET", "/replay/stream?file=sample_trades.jsonl&speed=1000") as response:
        body = "".join(response.iter_text())
    events = [chunk for chunk in body.split("\n\n") if chunk.startswith("event: ")]
    kinds = [e.split("\n")[0].removeprefix("event: ") for e in events]
    assert kinds[-1] == "done"
    assert "trades" in kinds and "progress" in kinds
    done = json.loads(events[-1].split("data: ", 1)[1])
    assert done["trades"] == 400
    for e in events:
        if e.startswith("event: anomaly"):
            anomaly = json.loads(e.split("data: ", 1)[1])
            assert anomaly["reasons"] and anomaly["context"][-1]["trade_id"] == anomaly["trade_id"]


def _first_trades(path, n):
    with open(path) as f:
        return [json.loads(next(f)) for _ in range(n)]


def test_metrics_endpoint(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "api_stream_clients" in response.text


def test_bad_filter_values_are_rejected(client):
    assert client.get("/anomalies?type=sideways").status_code == 422
    assert client.get("/trades?limit=0").status_code == 422
