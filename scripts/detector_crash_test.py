"""Crash test for the anomaly detector: delivery guarantee AND state recovery.

Replays a recorded trade file into a fresh Kafka topic while the real detector
(`python anomaly_detector.py`) is hard-killed and restarted over and over. The storage
consumer runs normally alongside it. Afterwards it checks Postgres against the file:

    missing / duplicates   every trade stored exactly once                  (must be 0)
    wrong scores           price_z, size_ratio and anomaly_type equal what ONE uninterrupted
                           run of the model produces                         (must be 0)

The second check is what proves the window rebuild: a detector that restarted with an empty
window would score the next trades differently (or not at all while warming up).

Uses its own topics, consumer groups and `crash_test` database. Needs docker compose up.

    python scripts/detector_crash_test.py --kills 100
    python scripts/detector_crash_test.py --file tests/data/sample_trades.jsonl --kills 10   # CI
"""

import argparse
import json
import math
import random
import re
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import psycopg2
from confluent_kafka.admin import AdminClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import anomaly_detector  # noqa: E402
from crash_test import (  # noqa: E402
    DB_NAME,
    Settings,
    create_topic,
    drop_database,
    setup_database,
    start_consumer,
    wait_for_log,
    wait_until_drained,
)
from replay import load_trades, replay  # noqa: E402

PHASES = {
    "batch received": "mid-batch (scoring and publishing)",
    "batch written": "results flushed, offset commit in flight",
    "offsets committed": "between batches",
}
EVENT = re.compile(r"batch received|batch written|offsets committed")


def expected_scores(trades):
    """What one uninterrupted run of the model gives for every trade."""
    windows = {}
    expected = {}
    for t in trades:
        window = windows.setdefault(t["s"], anomaly_detector.new_window())
        price_z, size_ratio = anomaly_detector.score_trade(window, float(t["p"]), float(t["q"]))
        expected[(t["s"], t["t"])] = (price_z, size_ratio,
                                      anomaly_detector.classify_anomaly(price_z, size_ratio))
    return expected


def same(a, b):
    if a is None or b is None:
        return a is b
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)


def verify(settings, trades):
    conn = psycopg2.connect(**{**settings.postgres_connect_kwargs(), "dbname": DB_NAME})
    with conn, conn.cursor() as cur:
        cur.execute("SELECT symbol, trade_id, price_z, size_ratio, anomaly_type FROM trades")
        rows = cur.fetchall()
    conn.close()

    expected = expected_scores(trades)
    counts = Counter((s, t) for s, t, *_ in rows)
    stored = {(s, t): (pz, sr, at) for s, t, pz, sr, at in rows}
    wrong = [k for k, (pz, sr, at) in stored.items() if k in expected and not (
        same(pz, expected[k][0]) and same(sr, expected[k][1]) and at == expected[k][2])]
    return {
        "trades_in_file": len(expected),
        "rows_in_postgres": len(rows),
        "missing": len(expected.keys() - stored.keys()),
        "duplicates": sum(n - 1 for n in counts.values() if n > 1),
        "wrong_scores": len(wrong),
        "anomalies": sum(1 for v in stored.values() if v[2]),
        "sample_wrong": [list(k) for k in wrong[:5]],
    }


def main():
    parser = argparse.ArgumentParser(description="Kill the anomaly detector repeatedly, then verify")
    parser.add_argument("--file", type=Path, default=ROOT / "data" / "history-60k.jsonl")
    parser.add_argument("--limit", type=int, help="only use the first N trades of the file")
    parser.add_argument("--kills", type=int, default=100)
    parser.add_argument("--rate", type=float, default=500.0, help="replay speed, trades per second")
    parser.add_argument("--max-uptime", type=float, default=0.4)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    settings = Settings()
    rng = random.Random(args.seed)
    trades = load_trades(args.file, args.limit)
    run_id = time.strftime("%Y%m%d-%H%M%S")
    trades_topic, analytics_topic = f"crash-det-{run_id}-trades", f"crash-det-{run_id}-analytics"
    detector_group, storage_group = f"crash-det-{run_id}-detector", f"crash-det-{run_id}-storage"
    out_dir = ROOT / "results" / f"detector-crash-test-{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Detector crash test {run_id}: {len(trades)} trades from {args.file.name} at "
          f"{args.rate:g}/s, {args.kills} kills")
    setup_database(settings)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for topic in (trades_topic, analytics_topic, f"{trades_topic}-dlq", f"{analytics_topic}-dlq"):
        create_topic(admin, topic, 3)

    common = {
        "KAFKA_BOOTSTRAP_SERVERS": settings.kafka_bootstrap_servers,
        "TRADES_TOPIC": trades_topic,
        "ANALYTICS_TOPIC": analytics_topic,
        "TRADES_DLQ_TOPIC": f"{trades_topic}-dlq",
        "ANALYTICS_DLQ_TOPIC": f"{analytics_topic}-dlq",
        "POSTGRES_DB": DB_NAME,
        "PYTHONUNBUFFERED": "1",
    }
    import os
    detector_env = {**os.environ, **common, "DETECTOR_GROUP_ID": detector_group,
                    "DETECTOR_GROUP_INSTANCE_ID": "crash-test-detector", "LOG_LEVEL": "DEBUG"}
    storage_env = {**os.environ, **common, "STORAGE_GROUP_ID": storage_group,
                   "STORAGE_BATCH_TIMEOUT_SECONDS": "0.5"}

    storage = start_consumer(storage_env, out_dir / "storage.log")
    feed = threading.Thread(
        target=replay, args=(trades, trades_topic, settings.kafka_bootstrap_servers),
        kwargs={"rate": args.rate, "progress": False}, daemon=True)
    feed.start()
    started = time.monotonic()
    phases = Counter()
    detector = None
    try:
        for n in range(1, args.kills + 1):
            if not feed.is_alive():
                # every trade is already in Kafka, so a restarted detector would soon have
                # nothing left to work on; more kills wouldn't test anything
                print(f"  feed finished after {n - 1} kills; raise --kills only with a slower --rate")
                break
            log_path = out_dir / f"detector-{n:03d}.log"
            detector = start_consumer(detector_env, log_path, "anomaly_detector.py")
            wait_for_log(detector, log_path, "batch received", timeout=60)
            time.sleep(rng.uniform(0, args.max_uptime))
            detector.kill()
            detector.wait()
            events = EVENT.findall(log_path.read_text(errors="replace"))
            phase = PHASES.get(events[-1] if events else "", "starting up")
            phases[phase] += 1
            rebuilt = log_path.read_text(errors="replace").count("Rebuilt windows")
            print(f"  kill {n:3d}/{args.kills}: died {phase:<42} (partitions rebuilt on start: {rebuilt})")

        print("Letting a final detector and the storage consumer drain everything...")
        feed.join()
        detector = start_consumer(detector_env, out_dir / "detector-final.log", "anomaly_detector.py")
        wait_until_drained(settings, trades_topic, detector_group, 3)
        wait_until_drained(settings, analytics_topic, storage_group, 3)
        result = verify(settings, trades)
    finally:
        for proc in (detector, storage):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait()
        admin.delete_topics([trades_topic, analytics_topic, f"{trades_topic}-dlq", f"{analytics_topic}-dlq"])
        drop_database(settings)

    result.update({"run_id": run_id, "kills": sum(phases.values()), "kill_phases": dict(phases),
                   "duration_seconds": round(time.monotonic() - started, 1), "settings": {
                       k: str(v) for k, v in vars(args).items()}})
    passed = result["missing"] == 0 and result["duplicates"] == 0 and result["wrong_scores"] == 0
    result["passed"] = passed
    (out_dir / "result.json").write_text(json.dumps(result, indent=2))

    print()
    print(f"Kills: {result['kills']} in {result['duration_seconds']}s")
    for phase, count in phases.most_common():
        print(f"  {count:3d} {phase}")
    print(f"Trades in file:     {result['trades_in_file']}")
    print(f"Rows in Postgres:   {result['rows_in_postgres']}")
    print(f"Missing:            {result['missing']}")
    print(f"Duplicates:         {result['duplicates']}")
    print(f"Wrong scores:       {result['wrong_scores']}   (vs one uninterrupted run of the model)")
    print(f"Anomalies flagged:  {result['anomalies']}")
    print(f"Result: {'PASS' if passed else 'FAIL'}   (details: {(out_dir / 'result.json').relative_to(ROOT)})")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
