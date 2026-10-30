"""Crash test for the storage consumer's delivery guarantee.

While synthetic trades flow into Kafka at a steady rate, this script starts the real
storage consumer (`python storage_consumer.py`), lets it start writing,
then hard-kills it at a random moment, over and over (default 100 times). A hard kill is
TerminateProcess on Windows / SIGKILL on Linux: no cleanup, no final commit, just like a
power cut or an OOM kill.

Afterwards it lets one last consumer drain the backlog and compares Postgres with the
list of trades Kafka acknowledged:

    missing     trades Kafka accepted that never reached the table   (must be 0)
    duplicates  (symbol, trade_id) pairs stored more than once       (must be 0)
    mismatched  rows whose price/quantity differ from what was sent  (must be 0)

It uses its own Kafka topic, consumer group and database (`crash_test`), so it never
touches the real pipeline's data. Needs Kafka and Postgres running (docker compose up -d).

Usage:
    python scripts/crash_test.py                  # 100 kills
    python scripts/crash_test.py --kills 10       # quick run (what CI does)
"""

import argparse
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

class Settings:
    """Connection details, read from the same env vars / .env as the services use."""

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    def postgres_connect_kwargs(self) -> dict[str, Any]:
        return {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.environ["POSTGRES_PORT"]),
            "dbname": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
        }


DB_NAME = "crash_test"
INSTANCE_ID = "crash-test-consumer"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

BATCH_EVENT = re.compile(r"batch (received|written|committed)|offsets committed")
WRITTEN = re.compile(r"batch written inserted=(\d+) updated=(\d+)")


@dataclass
class KillRecord:
    number: int
    seconds_after_first_batch: float
    phase: str  # what the consumer was doing when it died
    batches_committed: int
    rows_inserted: int
    rows_reupserted: int


# --------------------------------------------------------------------------- setup


def setup_database(settings: Settings, db_name: str = DB_NAME) -> None:
    admin = psycopg2.connect(**settings.postgres_connect_kwargs())
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {db_name}")
    admin.close()
    conn = psycopg2.connect(**{**settings.postgres_connect_kwargs(), "dbname": db_name})
    with conn, conn.cursor() as cur:
        cur.execute((ROOT / "schema.sql").read_text())
    conn.close()


def drop_database(settings: Settings, db_name: str = DB_NAME) -> None:
    admin = psycopg2.connect(**settings.postgres_connect_kwargs())
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)")
    admin.close()


def create_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    for future in futures.values():
        future.result(timeout=30)


# --------------------------------------------------------------------------- trade feed


class TradeFeed(threading.Thread):
    """Produces synthetic analytics records at a steady rate until stopped, and remembers
    exactly which ones Kafka acknowledged. Those are the trades that must end up in Postgres."""

    def __init__(self, bootstrap: str, topic: str, rate: float, seed: int) -> None:
        super().__init__(daemon=True)
        self.topic = topic
        self.rate = rate
        self.random = random.Random(seed)
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,  # no duplicates from producer retries
                "linger.ms": 5,
            }
        )
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.sent: dict[tuple[str, int], tuple[float, float]] = {}
        self.acked: set[tuple[str, int]] = set()
        self.failed = 0
        self.next_id = dict.fromkeys(SYMBOLS, 1)
        self.price = {"BTCUSDT": 65_000.0, "ETHUSDT": 3_200.0, "SOLUSDT": 150.0}

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        key = json.loads(msg.value() or b"")
        with self.lock:
            if err:
                self.failed += 1
            else:
                self.acked.add((key["symbol"], key["trade_id"]))

    def _make_record(self, symbol: str) -> dict[str, Any]:
        trade_id = self.next_id[symbol]
        self.next_id[symbol] += 1
        self.price[symbol] *= 1 + self.random.gauss(0, 0.0005)
        now_ms = time.time() * 1000
        is_anomaly = self.random.random() < 0.05
        return {
            "symbol": symbol,
            "trade_id": trade_id,
            "trade_time": int(now_ms),
            "received_at": now_ms,
            "price": round(self.price[symbol], 2),
            "quantity": round(self.random.expovariate(20), 6),
            "price_z": round(self.random.gauss(0, 1), 4),
            "volume_z": round(self.random.gauss(0, 1), 4),
            "is_anomaly": is_anomaly,
            "anomaly_type": self.random.choice(["price", "volume", "both"]) if is_anomaly else None,
        }

    def run(self) -> None:
        started = time.monotonic()
        produced = 0
        while not self.stop_event.is_set():
            due = int((time.monotonic() - started) * self.rate)
            while produced < due:
                record = self._make_record(SYMBOLS[produced % len(SYMBOLS)])
                with self.lock:
                    self.sent[(record["symbol"], record["trade_id"])] = (
                        record["price"],
                        record["quantity"],
                    )
                self.producer.produce(
                    self.topic,
                    key=record["symbol"].encode(),  # same symbol -> same partition
                    value=json.dumps(record).encode(),
                    on_delivery=self._on_delivery,
                )
                produced += 1
            self.producer.poll(0)
            time.sleep(0.01)
        self.producer.flush(30)

    def stop(self) -> None:
        self.stop_event.set()
        self.join()


# --------------------------------------------------------------------------- consumer process


def consumer_env(settings: Settings, topic: str, group: str, batch_size: int) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": settings.kafka_bootstrap_servers,
            "ANALYTICS_TOPIC": topic,
            "STORAGE_GROUP_ID": group,
            "STORAGE_GROUP_INSTANCE_ID": INSTANCE_ID,
            "STORAGE_BATCH_SIZE": str(batch_size),
            "STORAGE_BATCH_TIMEOUT_SECONDS": "0.5",
            "POSTGRES_DB": DB_NAME,
            "LOG_LEVEL": "DEBUG",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def start_consumer(
    env: dict[str, str], log_path: Path, script: str = "storage_consumer.py"
) -> subprocess.Popen[bytes]:
    log_file = log_path.open("wb")
    return subprocess.Popen(
        [sys.executable, script],
        cwd=ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )


def wait_for_log(
    proc: subprocess.Popen[bytes], log_path: Path, needle: str, timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if needle in log_path.read_text(errors="replace"):
            return
        if proc.poll() is not None:
            raise RuntimeError(f"consumer exited early, see {log_path}")
        time.sleep(0.02)
    raise TimeoutError(f"consumer never logged '{needle}' within {timeout}s, see {log_path}")


def summarise_log(log_path: Path) -> tuple[str, int, int, int]:
    """Returns (phase at death, batches committed, rows inserted, rows re-upserted)."""
    text = log_path.read_text(errors="replace")
    events = [m.group(0) for m in BATCH_EVENT.finditer(text)]
    last = events[-1] if events else ""
    phase = {
        "batch received": "mid-write (Postgres transaction open)",
        # can't tell from the log whether the offset commit had finished; the re-upsert
        # count on the next run is the ground truth
        "batch written": "after DB commit, offset commit in flight",
        "offsets committed": "between batches",
    }.get(last, "starting up")
    written = [(int(a), int(b)) for a, b in WRITTEN.findall(text)]
    return (
        phase,
        events.count("offsets committed"),
        sum(a for a, _ in written),
        sum(b for _, b in written),
    )


def wait_until_drained(settings: Settings, topic: str, group: str, partitions: int) -> None:
    probe = Consumer({"bootstrap.servers": settings.kafka_bootstrap_servers, "group.id": group})
    try:
        tps = [TopicPartition(topic, p) for p in range(partitions)]
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            drained = True
            for tp in probe.committed(tps, timeout=10):
                low, high = probe.get_watermark_offsets(TopicPartition(topic, tp.partition))
                # a partition that never received a message has no committed offset (< 0)
                position = tp.offset if tp.offset >= 0 else low
                drained = drained and position >= high
            if drained:
                return
            time.sleep(0.5)
        raise TimeoutError("final consumer did not catch up within 300s")
    finally:
        probe.close()


# --------------------------------------------------------------------------- verification


def verify(settings: Settings, feed: TradeFeed) -> dict[str, Any]:
    conn = psycopg2.connect(**{**settings.postgres_connect_kwargs(), "dbname": DB_NAME})
    with conn, conn.cursor() as cur:
        cur.execute("SELECT symbol, trade_id, price, quantity FROM trades")
        rows = cur.fetchall()
    conn.close()

    keys = Counter((symbol, trade_id) for symbol, trade_id, _, _ in rows)
    stored = {(s, t): (p, q) for s, t, p, q in rows}
    missing = feed.acked - stored.keys()
    unexpected = stored.keys() - feed.acked
    # prices are stored as exact NUMERIC, so compare against the exact decimal that was sent
    mismatched = [
        k for k in feed.acked & stored.keys()
        if stored[k] != tuple(Decimal(str(v)) for v in feed.sent[k])
    ]
    return {
        "trades_acknowledged_by_kafka": len(feed.acked),
        "trades_failed_to_produce": feed.failed,
        "rows_in_postgres": len(rows),
        "missing": len(missing),
        "duplicates": sum(n - 1 for n in keys.values() if n > 1),
        "unexpected": len(unexpected),
        "mismatched_values": len(mismatched),
        "sample_missing": sorted(missing)[:10],
    }


# --------------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--kills", type=int, default=100)
    # The rate is set high on purpose: each restart then starts with a backlog of full
    # batches, so the random kill lands while the consumer is actually writing.
    parser.add_argument("--rate", type=float, default=2000.0, help="trades per second")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument(
        "--max-uptime",
        type=float,
        default=0.4,
        help="kill at a random time between 0 and this many seconds after the first batch",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keep-db", action="store_true", help="leave the crash_test database")
    args = parser.parse_args()

    settings = Settings()
    rng = random.Random(args.seed)
    run_id = time.strftime("%Y%m%d-%H%M%S")
    topic = group = f"crash-test-{run_id}"
    out_dir = ROOT / "results" / f"crash-test-{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"Crash test {run_id}: {args.kills} kills, {args.rate:.0f} trades/s, "
        f"batch size {args.batch_size}, {args.partitions} partitions"
    )
    setup_database(settings)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    create_topic(admin, topic, args.partitions)

    env = consumer_env(settings, topic, group, args.batch_size)
    feed = TradeFeed(settings.kafka_bootstrap_servers, topic, args.rate, args.seed)
    feed.start()
    started = time.monotonic()
    kills: list[KillRecord] = []
    proc: subprocess.Popen[bytes] | None = None

    try:
        for n in range(1, args.kills + 1):
            log_path = out_dir / f"consumer-{n:03d}.log"
            proc = start_consumer(env, log_path)
            wait_for_log(proc, log_path, "batch received", timeout=60)
            delay = rng.uniform(0, args.max_uptime)
            time.sleep(delay)
            proc.kill()
            proc.wait()
            phase, committed, inserted, reupserted = summarise_log(log_path)
            kills.append(KillRecord(n, round(delay, 3), phase, committed, inserted, reupserted))
            print(
                f"  kill {n:3d}/{args.kills}: {delay:4.2f}s in, died {phase:<41} "
                f"(batches committed this run: {committed})"
            )

        print("Stopping the trade feed and letting a final consumer drain the backlog...")
        feed.stop()
        final_log = out_dir / "consumer-final.log"
        proc = start_consumer(env, final_log)
        wait_until_drained(settings, topic, group, args.partitions)
        proc.kill()
        proc.wait()
        _, _, final_inserted, final_reupserted = summarise_log(final_log)

        result = verify(settings, feed)
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()
        if feed.is_alive():
            feed.stop()
        admin.delete_topics([topic])
        if not args.keep_db:
            drop_database(settings)

    phases = Counter(k.phase for k in kills)
    result.update(
        {
            "run_id": run_id,
            "kills": len(kills),
            "duration_seconds": round(time.monotonic() - started, 1),
            "kill_phases": dict(phases),
            "rows_reupserted_after_redelivery": sum(k.rows_reupserted for k in kills)
            + final_reupserted,
            "settings": vars(args),
        }
    )
    passed = result["missing"] == 0 and result["duplicates"] == 0 and result["unexpected"] == 0
    passed = passed and result["mismatched_values"] == 0
    result["passed"] = passed

    report = out_dir / "result.json"
    report.write_text(
        json.dumps({**result, "kill_log": [asdict(k) for k in kills]}, indent=2, default=str)
    )

    print()
    print(f"Kills: {len(kills)} in {result['duration_seconds']}s")
    for phase, count in phases.most_common():
        print(f"  {count:3d} {phase}")
    print(f"Trades acknowledged by Kafka: {result['trades_acknowledged_by_kafka']}")
    print(f"Rows in Postgres:             {result['rows_in_postgres']}")
    print(f"Missing:                      {result['missing']}")
    print(f"Duplicates:                   {result['duplicates']}")
    print(f"Unexpected / mismatched:      {result['unexpected']} / {result['mismatched_values']}")
    print(f"Redelivered rows absorbed by the upsert: {result['rows_reupserted_after_redelivery']}")
    print(f"Result: {'PASS' if passed else 'FAIL'}   (details: {report.relative_to(ROOT)})")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
