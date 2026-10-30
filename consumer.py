import json
import os

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "crypto-trades"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "crypto-consumer-group",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([TOPIC])

print(f"Listening on topic '{TOPIC}'...")

try:
    while True:
        msg = consumer.poll(1.0)    # Poll data from kafka every 1 sec
        if not msg:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        trade = json.loads(msg.value().decode("utf-8"))
        price = float(trade["p"])
        quantity = float(trade["q"])
        print(f"Price: {price:.2f}  Quantity: {quantity:.5f}")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
