"""One end-to-end test: a real browser, the real API and a throwaway Postgres (needs Docker and Playwright)."""

import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest
import requests

from storage_consumer import to_row, write_batch
from tests.conftest import make_record

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def api_url(postgres_url, conn, tmp_path):
    """Starts the API the way the README does, against the test database."""
    url = urlparse(postgres_url)
    port = free_port()
    env = {
        **os.environ,
        "POSTGRES_HOST": url.hostname,
        "POSTGRES_PORT": str(url.port),
        "POSTGRES_DB": url.path.lstrip("/"),
        "POSTGRES_USER": url.username,
        "POSTGRES_PASSWORD": url.password,
    }
    with open(tmp_path / "api.log", "w") as log:
        api = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--port", str(port)],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=log,
        )
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                if requests.get(f"{base}/readyz", timeout=2).status_code == 200:
                    break
            except requests.RequestException:
                pass
            assert time.monotonic() < deadline, "API didn't start:\n" + (tmp_path / "api.log").read_text()
            time.sleep(0.5)
        yield base
    finally:
        api.terminate()
        api.wait(timeout=10)


def test_analyst_sees_a_flagged_trade_and_can_run_a_replay(api_url, conn):
    sync_api = pytest.importorskip("playwright.sync_api")
    write_batch(
        conn,
        [to_row(make_record(i), datetime.now(timezone.utc)) for i in range(1, 6)]
        + [
            to_row(
                make_record(6, is_anomaly=True, anomaly_type="price", price_z=8.0, size_ratio=3.0),
                datetime.now(timezone.utc),
            )
        ],
    )

    with sync_api.sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        errors = []
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))

        page.goto(f"{api_url}/dashboard/")
        sync_api.expect(page.get_by_text("Market surveillance")).to_be_visible()
        # the stored anomaly reaches the list, and its z-score shows
        sync_api.expect(page.get_by_text("z +8.0").first).to_be_visible(timeout=15_000)

        page.click("#mode-replay")
        page.select_option("#replay-file", "sample_trades.jsonl")
        page.get_by_role("button", name="1000×").click()
        page.click("#replay-start")
        sync_api.expect(page.locator("#replay-status")).to_contain_text("Finished: 3,000 trades", timeout=60_000)

        browser.close()

    assert errors == []
