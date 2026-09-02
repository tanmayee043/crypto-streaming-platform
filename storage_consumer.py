import json
import psycopg2
from confluent_kafka import Consumer
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "trade-analytics"

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

print(f"Writing '{TOPIC}' to Postgres...")

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

        cur.execute("""
            INSERT INTO trades (symbol, trade_id, trade_time, price, quantity, price_z, volume_z, is_anomaly, anomaly_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_id) DO NOTHING
        """, (r["symbol"], r["trade_id"], trade_time, r["price"], r["quantity"],
            r["price_z"], r["volume_z"], r["is_anomaly"], r["anomaly_type"]))
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cur.close()
    conn.close()