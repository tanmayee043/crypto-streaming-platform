import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import main
from rate_limit import RateLimitMiddleware, TokenBucket


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


def test_bucket_allows_a_burst_then_blocks_then_refills():
    clock = Clock()
    bucket = TokenBucket(per_minute=60, clock=clock)
    assert all(bucket.allow("1.1.1.1") for _ in range(60))
    assert not bucket.allow("1.1.1.1")
    clock.now += 2  # 60 per minute = one token a second
    assert bucket.allow("1.1.1.1")
    assert bucket.allow("1.1.1.1")
    assert not bucket.allow("1.1.1.1")


def test_each_ip_has_its_own_bucket():
    bucket = TokenBucket(per_minute=1, clock=Clock())
    assert bucket.allow("1.1.1.1")
    assert not bucket.allow("1.1.1.1")
    assert bucket.allow("2.2.2.2")


@pytest.fixture
def limited():
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, per_minute=2)
    app.get("/trades")(lambda: {"ok": True})
    app.get("/healthz")(lambda: {"ok": True})
    return TestClient(app)


def test_middleware_answers_429_with_retry_after(limited):
    assert [limited.get("/trades").status_code for _ in range(3)] == [200, 200, 429]
    blocked = limited.get("/trades")
    assert blocked.headers["retry-after"] == "30"
    assert "slow down" in blocked.json()["detail"]


def test_uptime_checks_are_never_limited(limited):
    assert {limited.get("/healthz").status_code for _ in range(10)} == {200}


def test_too_many_replays_are_refused(monkeypatch):
    monkeypatch.setitem(main.open_replays, "testclient", main.MAX_REPLAYS_PER_IP)
    response = TestClient(main.app).get("/replay/stream?file=sample_trades.jsonl")
    assert response.status_code == 429


def test_too_many_streams_are_refused(monkeypatch):
    monkeypatch.setitem(main.open_streams, "testclient", main.MAX_STREAMS_PER_IP)
    assert TestClient(main.app).get("/stream").status_code == 429


def test_home_redirects_to_the_dashboard():
    response = TestClient(main.app).get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/dashboard/"
