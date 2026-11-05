"""The API against a real Postgres (needs Docker): history filters, detail, health and live push."""

import asyncio
from datetime import datetime, timezone
from urllib.parse import urlparse

import pytest
from fastapi.testclient import TestClient

import gap_checker
import main
from storage_consumer import to_row, write_batch
from tests.conftest import make_record

pytestmark = pytest.mark.integration


@pytest.fixture
def api(postgres_url, conn, monkeypatch):
    url = urlparse(postgres_url)
    for key, value in {"POSTGRES_HOST": url.hostname, "POSTGRES_PORT": str(url.port),
                       "POSTGRES_DB": url.path.lstrip("/"), "POSTGRES_USER": url.username,
                       "POSTGRES_PASSWORD": url.password}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(gap_checker, "consumer_lag", lambda groups=None: {})  # no Kafka in this test
    monkeypatch.setattr(main, "latest_health", None)
    return TestClient(main.app)


def store(conn, records):
    write_batch(conn, [to_row(r, datetime.now(timezone.utc)) for r in records])


def anomaly(trade_id, symbol="BTCUSDT", kind="price"):
    return make_record(trade_id, symbol=symbol, is_anomaly=True, anomaly_type=kind, price_z=8.0, size_ratio=3.0)


def test_history_filters_and_cursor_pagination(api, conn):
    store(conn, [make_record(i) for i in range(1, 6)]
          + [anomaly(i) for i in range(6, 11)] + [anomaly(20, symbol="ETHUSDT", kind="volume")])
    assert api.get("/anomalies/count").json()["count"] == 6
    assert api.get("/anomalies/count?symbol=BTCUSDT&type=price").json()["count"] == 5
    assert [r["symbol"] for r in api.get("/anomalies?type=volume").json()] == ["ETHUSDT"]

    page1 = api.get("/anomalies?symbol=BTCUSDT&limit=3").json()
    page2 = api.get(f"/anomalies?symbol=BTCUSDT&limit=3&before_id={page1[-1]['id']}").json()
    assert [r["trade_id"] for r in page1 + page2] == [10, 9, 8, 7, 6]


def test_date_range_filter(api, conn):
    store(conn, [anomaly(1)])  # trade_time is in 2023 (see make_record)
    assert api.get("/anomalies/count?start=2023-01-01T00:00:00Z&end=2024-01-01T00:00:00Z").json()["count"] == 1
    assert api.get("/anomalies/count?start=2024-01-01T00:00:00Z").json()["count"] == 0


def test_anomaly_detail_has_context_and_reasons(api, conn):
    store(conn, [make_record(i) for i in range(1, 40)] + [anomaly(40)] + [make_record(i) for i in range(41, 50)])
    detail = api.get("/anomalies/BTCUSDT/40").json()
    assert detail["trade"]["trade_id"] == 40
    assert [r["trade_id"] for r in detail["context"]] == list(range(10, 50))  # 30 before, the trade, 9 after
    assert detail["reasons"][0]["signal"] == "price" and detail["reasons"][0]["flagged"] is True
    assert api.get("/anomalies/BTCUSDT/999").status_code == 404


def test_pipeline_health_and_readiness(api, conn):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    store(conn, [make_record(i, trade_time=now_ms) for i in range(1, 11)])  # completeness covers the last 24 h
    health = api.get("/health/pipeline").json()
    assert health["completeness"][0]["completeness_pct"] == 100.0
    assert health["trades_per_second"] == pytest.approx(10 / 60, abs=0.01)
    assert health["status"] in ("ok", "degraded")
    assert api.get("/readyz").json() == {"status": "ready"}


def test_live_loop_pushes_new_rows_to_connected_clients(api, conn):
    store(conn, [make_record(1)])

    class QuietAlerter:
        def anomalies(self, rows):
            pass

        def health(self, health):
            pass

    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        main.subscribers.add(queue)
        try:
            last_id, last_health = await main.broadcast_once(QuietAlerter(), None, 0.0)  # starts at the newest row
            store(conn, [make_record(2), anomaly(3)])
            await main.broadcast_once(QuietAlerter(), last_id, last_health)
            events = []
            while not queue.empty():
                events.append(queue.get_nowait())
            return events
        finally:
            main.subscribers.discard(queue)

    events = asyncio.run(run())
    trades = [payload for kind, payload in events if kind == "trades"]
    assert [r["trade_id"] for r in trades[0]] == [2, 3]
    assert any(kind == "health" for kind, _ in events)
