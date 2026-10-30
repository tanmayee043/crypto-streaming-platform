"""Replays a recorded trade file into Kafka, so the whole pipeline runs on known data.

    python scripts/replay.py data/history-60k.jsonl --speed 100     # 100x faster than real time
    python scripts/replay.py data/history-60k.jsonl --speed 0       # as fast as possible (load test)
    python scripts/replay.py data/history-60k.jsonl --rate 1000     # a steady 1,000 trades/s

With --speed, the gaps between trades follow their original exchange timestamps (T), divided
by the speed. Each trade is stamped with `received_at` when it is sent, exactly like the live
producer, so latency numbers mean the same thing. Messages are keyed by symbol, like live.
Record files with scripts/record_trades.py.
"""

import argparse
import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def load_trades(path, limit=None):
    trades = []
    with open(path) as f:
        for line in f:
            if line.strip():
                trades.append(json.loads(line))
                if limit and len(trades) >= limit:
                    break
    return trades


def send_times(trades, speed=None, rate=None):
    """Seconds after the start at which each trade should be sent."""
    if rate:
        return [i / rate for i in range(len(trades))]
    if not speed:
        return [0.0] * len(trades)  # as fast as possible
    start = trades[0]["T"]
    return [(t["T"] - start) / 1000 / speed for t in trades]


def replay(trades, topic, bootstrap, speed=None, rate=None, progress=True):
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 5})
    failed = []
    schedule = send_times(trades, speed, rate)
    started = time.monotonic()
    for i, (trade, due) in enumerate(zip(trades, schedule, strict=True)):
        wait = due - (time.monotonic() - started)
        if wait > 0:
            time.sleep(wait)
        message = dict(trade)
        message["received_at"] = time.time() * 1000
        while True:
            try:
                producer.produce(topic, key=message["s"].encode(), value=json.dumps(message).encode(),
                                 on_delivery=lambda err, _msg: err and failed.append(err))
                break
            except BufferError:
                producer.poll(0.5)  # local buffer full: let Kafka catch up
        producer.poll(0)
        if progress and i and i % 10000 == 0:
            print(f"  sent {i}/{len(trades)}")
    producer.flush(60)
    elapsed = time.monotonic() - started
    return {"sent": len(trades) - len(failed), "failed": len(failed), "seconds": round(elapsed, 2),
            "rate": round(len(trades) / elapsed, 1) if elapsed else None}


def main():
    parser = argparse.ArgumentParser(description="Replay recorded trades into Kafka")
    parser.add_argument("file", type=Path)
    pace = parser.add_mutually_exclusive_group()
    pace.add_argument("--speed", type=float, default=0,
                      help="time multiplier, e.g. 10, 100, 1000 (0 = as fast as possible)")
    pace.add_argument("--rate", type=float, help="fixed trades per second instead of original timing")
    parser.add_argument("--limit", type=int, help="only replay the first N trades")
    parser.add_argument("--topic", default=os.getenv("TRADES_TOPIC", "crypto-trades"))
    args = parser.parse_args()

    trades = load_trades(args.file, args.limit)
    span = (trades[-1]["T"] - trades[0]["T"]) / 1000
    pace = f"{args.rate:g} trades/s" if args.rate else (f"{args.speed:g}x" if args.speed else "max speed")
    print(f"Replaying {len(trades)} trades ({span / 3600:.1f}h of market time) into '{args.topic}' at {pace}")
    result = replay(trades, args.topic, os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    args.speed, args.rate)
    print(f"Sent {result['sent']} trades in {result['seconds']}s ({result['rate']} trades/s), "
          f"{result['failed']} failed")


if __name__ == "__main__":
    main()
