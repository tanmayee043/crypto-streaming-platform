import json
import statistics
from collections import deque
from confluent_kafka import Consumer, Producer

import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("anomaly_detector")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "crypto-trades"
WINDOW_SIZE = 30
Z_SCORE_THRESHOLD = 3.0      # max z-score how many std devs away counts as "unusual"

OUTPUT_TOPIC = "trade-analytics"

METRICS_INTERVAL_SECONDS = 60

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "crypto-anomaly-detector",
    "auto.offset.reset": "latest",
})
consumer.subscribe([INPUT_TOPIC])

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def producer_delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

# fixed-size window: deque with maxlen automatically drops the oldest entry
window = deque(maxlen = WINDOW_SIZE)

def z_score(value, values):
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)  # population stdev — we have the whole window, not a sample
    if stdev == 0:
        return 0.0
    return (value - mean) / stdev      # z-score

# --- metrics ---
processed_count = 0
anomaly_count = 0
last_metrics_log = time.time()

def maybe_log_metrics():
    global processed_count, anomaly_count, last_metrics_log
    now = time.time()
    elapsed = now - last_metrics_log
    if elapsed >= METRICS_INTERVAL_SECONDS:
        rate = anomaly_count / processed_count * 100 if processed_count else 0
        log.info(
            f"Processed {processed_count} trades in {elapsed:.0f}s, "
            f"{anomaly_count} anomalies flagged ({rate:.1f}%)"
        )
        processed_count = 0
        anomaly_count = 0
        last_metrics_log = now

log.info(f"Watching '{INPUT_TOPIC}' — window={WINDOW_SIZE} trades, threshold={Z_SCORE_THRESHOLD} std dev")
log.info(f"Publishing results to '{OUTPUT_TOPIC}'")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        trade = json.loads(msg.value().decode("utf-8"))
        price = float(trade["p"])
        quantity = float(trade["q"])
        
        window.append((price, quantity))

        if len(window) < WINDOW_SIZE:
            print(f"Price: {price:.2f}  Qty: {quantity:.5f}  (warming up: {len(window)}/{WINDOW_SIZE})")
            continue

        prices = [p for p, _ in window]
        volumes = [q for _, q in window]

        price_z = z_score(price, prices)
        vol_z = z_score(quantity, volumes)

        anomaly_type = None
        if abs(price_z) > Z_SCORE_THRESHOLD and abs(vol_z) > Z_SCORE_THRESHOLD:
            anomaly_type = "both"
        elif abs(price_z) > Z_SCORE_THRESHOLD:
            anomaly_type = "price"
        elif abs(vol_z) > Z_SCORE_THRESHOLD:
            anomaly_type = "volume"

        is_anomaly = anomaly_type is not None
        processed_count += 1
        if is_anomaly:
            anomaly_count += 1
            log.info(f"🚨 {anomaly_type.upper()} ANOMALY — price={price:.2f}, qty={quantity:.5f}, "
                      f"price_z={price_z:.2f}, vol_z={vol_z:.2f}")

        # Publish the processed result downstream for storage_consumer.py to persist
        result = {
            "symbol": trade["s"],
            "trade_id": trade["t"],
            "trade_time": trade["T"],  # Binance sends epoch milliseconds
            "received_at": trade["received_at"],   
            "price": price,
            "quantity": quantity,
            "price_z": price_z,
            "volume_z": vol_z,
            "is_anomaly": is_anomaly,
            "anomaly_type": anomaly_type,   # "price" | "volume" | "both" | None
        }
        producer.produce(
            OUTPUT_TOPIC,
            value=json.dumps(result).encode("utf-8"),
            callback=producer_delivery_report,
        )
        producer.poll(0)
        maybe_log_metrics()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.flush()