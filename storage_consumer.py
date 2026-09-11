import json
import psycopg2
from confluent_kafka import Consumer
from datetime import datetime, timezone

import logging
import time

import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("storage_consumer")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "trade-analytics"

METRICS_INTERVAL_SECONDS = 60

conn = psycopg2.connect(
    host="localhost",
    port=int(os.environ["POSTGRES_PORT"]),
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)
conn.autocommit = True
cur = conn.cursor()

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "storage-writer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

# --- metrics: end-to-end latency from Binance trade_time to this write ---
write_count = 0
latencies_ms = []
last_metrics_log = time.time()

def maybe_log_metrics():
    global write_count, latencies_ms, last_metrics_log
    now = time.time()
    elapsed = now - last_metrics_log
    if elapsed >= METRICS_INTERVAL_SECONDS:
        if latencies_ms:
            latencies_ms.sort()
            p50 = latencies_ms[len(latencies_ms) // 2]
            p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
            log.info(
                f"Wrote {write_count} rows in {elapsed:.0f}s | "
                f"latency p50={p50:.0f}ms p95={p95:.0f}ms"
            )
        write_count = 0
        latencies_ms = []
        last_metrics_log = now

log.info(f"Writing '{TOPIC}' to Postgres...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        r = json.loads(msg.value().decode("utf-8"))
        trade_time = datetime.fromtimestamp(r["trade_time"] / 1000, tz=timezone.utc)

        # latency = time from OUR OWN receipt of the trade to this write — same clock on both ends,
        # avoids the clock-skew issue from comparing against Binance's server timestamp
        latency_ms = time.time() * 1000 - r["received_at"]
        latencies_ms.append(latency_ms)

        try:
            cur.execute("""
                INSERT INTO trades (symbol, trade_id, trade_time, price, quantity, price_z, volume_z, is_anomaly, anomaly_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, trade_id) DO NOTHING
            """, (r["symbol"], r["trade_id"], trade_time, r["price"], r["quantity"],
                r["price_z"], r["volume_z"], r["is_anomaly"], r["anomaly_type"]))
            write_count += 1
        except Exception as e:
            log.error(f"Failed to write trade {r.get('trade_id')}: {e}")

        maybe_log_metrics()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cur.close()
    conn.close()