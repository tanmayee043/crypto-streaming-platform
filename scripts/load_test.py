"""Load test: replays a recorded file through the real detector and storage consumer.

    python scripts/load_test.py --rate 1000      # steady 1,000 trades/s: latency under load
    python scripts/load_test.py --rate 0         # as fast as possible: maximum throughput

It starts `anomaly_detector.py` and `storage_consumer.py` on fresh topics and a separate
`load_test` database, replays the file, waits until every trade is in Postgres, then reports:

    throughput   trades stored / time from the first send to the last write
    latency      stored_at - received_at per trade (p50/p95/p99/max). Both timestamps come
                 from this machine's clock, so there is no clock skew in the number.

Needs docker compose up. Uses data/history-60k.jsonl by default (scripts/record_trades.py).
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import psycopg2
from confluent_kafka.admin import AdminClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from crash_test import Settings, create_topic, drop_database, setup_database, start_consumer  # noqa: E402
from replay import load_trades, replay  # noqa: E402

DB_NAME = "load_test"

LATENCY_SQL = """
    SELECT count(*),
           percentile_cont(0.50) WITHIN GROUP (ORDER BY ms),
           percentile_cont(0.95) WITHIN GROUP (ORDER BY ms),
           percentile_cont(0.99) WITHIN GROUP (ORDER BY ms),
           max(ms),
           extract(epoch FROM max(stored_at) - min(received_at))
    FROM (SELECT stored_at, received_at,
                 extract(epoch FROM stored_at - received_at) * 1000 AS ms FROM trades) t
"""


def histogram_quantile(metrics_text, name, q):
    """The q-quantile (upper bucket bound) of a Prometheus histogram, from a /metrics page."""
    buckets = []
    for line in metrics_text.splitlines():
        if line.startswith(f'{name}_bucket{{le="') and "+Inf" not in line:
            bound = float(line.split('le="')[1].split('"')[0])
            buckets.append((bound, float(line.rsplit(" ", 1)[1])))
    total = next(
        (float(line.rsplit(" ", 1)[1]) for line in metrics_text.splitlines() if line.startswith(f"{name}_count")), 0
    )
    for bound, count in buckets:
        if total and count >= q * total:
            return bound
    return None


def stage_latency(port, name):
    """p50 and p95 upper bounds in ms for one stage, read from the service while it's running."""
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        text = response.read().decode()
    return {
        f"p{int(q * 100)}_ms_at_most": (bound * 1000 if (bound := histogram_quantile(text, name, q)) else None)
        for q in (0.5, 0.95)
    }


def count_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM trades")
        return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description="End-to-end load test with replayed trades")
    parser.add_argument("--file", type=Path, default=ROOT / "data" / "history-60k.jsonl")
    parser.add_argument("--rate", type=float, default=1000, help="trades per second (0 = max speed)")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    settings = Settings()
    trades = load_trades(args.file, args.limit)
    run_id = time.strftime("%Y%m%d-%H%M%S")
    trades_topic, analytics_topic = f"load-{run_id}-trades", f"load-{run_id}-analytics"
    out_dir = ROOT / "results" / f"load-test-{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    topics = [trades_topic, analytics_topic, f"{trades_topic}-dlq", f"{analytics_topic}-dlq"]

    setup_database(settings, DB_NAME)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for topic in topics:
        create_topic(admin, topic, 3)
    env = {
        **os.environ,
        "KAFKA_BOOTSTRAP_SERVERS": settings.kafka_bootstrap_servers,
        "TRADES_TOPIC": trades_topic,
        "ANALYTICS_TOPIC": analytics_topic,
        "TRADES_DLQ_TOPIC": topics[2],
        "ANALYTICS_DLQ_TOPIC": topics[3],
        "POSTGRES_DB": DB_NAME,
        "DETECTOR_GROUP_ID": f"load-{run_id}-detector",
        "STORAGE_GROUP_ID": f"load-{run_id}-storage",
        "METRICS_INTERVAL_SECONDS": "5",
        "PYTHONUNBUFFERED": "1",
    }
    procs = [
        start_consumer({**env, "METRICS_PORT": "8102"}, out_dir / "detector.log", "anomaly_detector.py"),
        start_consumer({**env, "METRICS_PORT": "8103"}, out_dir / "storage.log", "storage_consumer.py"),
    ]
    conn = psycopg2.connect(**{**settings.postgres_connect_kwargs(), "dbname": DB_NAME})
    try:
        time.sleep(5)  # let both consumers join their groups before the first trade arrives
        pace = f"{args.rate:g} trades/s" if args.rate else "max speed"
        print(f"Load test {run_id}: {len(trades)} trades at {pace}")
        sent = replay(trades, trades_topic, settings.kafka_bootstrap_servers, rate=args.rate or None)
        print(f"  replay sent {sent['sent']} trades in {sent['seconds']}s ({sent['rate']} trades/s)")
        deadline = time.monotonic() + 600
        while (stored := count_rows(conn)) < sent["sent"]:
            if time.monotonic() > deadline:
                raise TimeoutError(f"only {stored}/{sent['sent']} trades stored after 10 minutes")
            time.sleep(0.5)
        with conn.cursor() as cur:
            cur.execute(LATENCY_SQL)
            n, p50, p95, p99, worst, span = (float(v) for v in cur.fetchone())  # NUMERIC -> float
            n = int(n)
        stages = {
            "detector": stage_latency(8102, "detector_stage_latency_seconds"),
            "detector_batch": stage_latency(8102, "detector_batch_seconds"),
            "storage": stage_latency(8103, "storage_stage_latency_seconds"),
            "storage_write": stage_latency(8103, "storage_write_seconds"),
        }
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()
        conn.close()
        admin.delete_topics(topics)
        drop_database(settings, DB_NAME)

    result = {
        "run_id": run_id,
        "trades": n,
        "target_rate": args.rate,
        "send_rate": sent["rate"],
        "throughput_per_s": round(n / span, 1),
        "latency_ms": {"p50": round(p50, 1), "p95": round(p95, 1), "p99": round(p99, 1), "max": round(worst, 1)},
        "stage_latency": stages,
    }
    (out_dir / "result.json").write_text(json.dumps(result, indent=2))
    print(f"Stored {n} trades in {span:.1f}s end to end -> {result['throughput_per_s']} trades/s")
    print(f"Latency (receive -> stored): p50 {p50:.0f} ms, p95 {p95:.0f} ms, p99 {p99:.0f} ms, max {worst:.0f} ms")
    for stage, values in stages.items():
        print(f"  {stage}: {values}")


if __name__ == "__main__":
    main()
