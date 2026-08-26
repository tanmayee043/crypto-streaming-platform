import json
import statistics
import time
from collections import deque
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "crypto-trades"
WINDOW_SECONDS = 300         # Good value: 60 # how far back we look
MIN_SAMPLES = 20             # Good value: 30 # don't flag anomalies until we have enough data to trust the average
Z_SCORE_THRESHOLD = 3.0      # max z-score how many std devs away counts as "unusual"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "crypto-anomaly-detector",
    "auto.offset.reset": "latest",
})
consumer.subscribe([TOPIC])

# sliding window: each entry is (timestamp, price, quantity)
window = deque()

def prune_old(now):
    while window and now - window[0][0] > WINDOW_SECONDS:
        window.popleft()

def z_score(value, values):
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)  # population stdev — we have the whole window, not a sample
    if stdev == 0:
        return 0.0
    return (value - mean) / stdev      # z-score

print(f"Watching '{TOPIC}' — window={WINDOW_SECONDS}s, threshold={Z_SCORE_THRESHOLD} std dev")

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
        now = time.time()

        #debug code
        if window:
            gap = now - window[-1][0]
            print(f"[{gap:.1f}s since last trade]", end=" ")

        window.append((now, price, quantity))
        prune_old(now)

        if len(window) < MIN_SAMPLES:
            print(f"Price: {price:.2f}  Qty: {quantity:.5f}  (warming up: {len(window)}/{MIN_SAMPLES})")
            continue

        prices = [p for _, p, _ in window]
        volumes = [q for _, _, q in window]

        price_z = z_score(price, prices)
        vol_z = z_score(quantity, volumes)

        flags = []
        if abs(price_z) > Z_SCORE_THRESHOLD:
            flags.append(f"PRICE ANOMALY (z={price_z:.2f})")
        if abs(vol_z) > Z_SCORE_THRESHOLD:
            flags.append(f"VOLUME ANOMALY (z={vol_z:.2f})")

        if flags:
            print(f"🚨 {' | '.join(flags)} — price={price:.2f}, qty={quantity:.5f}")
        else:
            print(f"Price: {price:.2f}  Qty: {quantity:.5f}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()