import json
import statistics
from collections import deque
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "crypto-trades"
WINDOW_SIZE = 30
Z_SCORE_THRESHOLD = 3.0      # max z-score how many std devs away counts as "unusual"

OUTPUT_TOPIC = "trade-analytics"

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

print(f"Watching '{INPUT_TOPIC}' — window={WINDOW_SIZE} trades, threshold={Z_SCORE_THRESHOLD} std dev")
print(f"Publishing results to '{OUTPUT_TOPIC}'")

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

        flags = []
        if abs(price_z) > Z_SCORE_THRESHOLD:
            flags.append(f"PRICE ANOMALY (z={price_z:.2f})")
        if abs(vol_z) > Z_SCORE_THRESHOLD:
            flags.append(f"VOLUME ANOMALY (z={vol_z:.2f})")

        if flags:
            print(f"🚨 {' | '.join(flags)} — price={price:.2f}, qty={quantity:.5f}")
        else:
            print(f"Price: {price:.2f}  Qty: {quantity:.5f}")


        # Publish the processed result downstream for storage_consumer.py to persist
        result = {
            "symbol": trade["s"],
            "trade_id": trade["t"],
            "trade_time": trade["T"],  # Binance sends epoch milliseconds
            "price": price,
            "quantity": quantity,
            "price_z": price_z,
            "volume_z": vol_z,
            "is_anomaly": bool(flags),
        }
        producer.produce(
            OUTPUT_TOPIC,
            value=json.dumps(result).encode("utf-8"),
            callback=producer_delivery_report,
        )
        producer.poll(0)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    producer.flush()