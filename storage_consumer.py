import json
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaException, Producer
from datetime import datetime, timezone
from decimal import Decimal

import logging
import time

import os
from dotenv import load_dotenv
load_dotenv()

from kafka_helpers import consume_batch, next_offsets, send_to_dead_letter  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("storage_consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("ANALYTICS_TOPIC", "trade-analytics")
DEAD_LETTER_TOPIC = os.getenv("ANALYTICS_DLQ_TOPIC", "trade-analytics-dlq")
GROUP_ID = os.getenv("STORAGE_GROUP_ID", "storage-writer")
# optional Kafka "static membership": a restarted consumer gets its partitions back
# immediately instead of waiting for a group rebalance
GROUP_INSTANCE_ID = os.getenv("STORAGE_GROUP_INSTANCE_ID")

BATCH_SIZE = int(os.getenv("STORAGE_BATCH_SIZE", "500"))
BATCH_TIMEOUT_SECONDS = float(os.getenv("STORAGE_BATCH_TIMEOUT_SECONDS", "1.0"))
MAX_RETRY_BACKOFF_SECONDS = 30

METRICS_INTERVAL_SECONDS = float(os.getenv("METRICS_INTERVAL_SECONDS", "60"))

# Delivery guarantee: offsets are committed only AFTER a batch is safely in Postgres.
# If we crash before that, Kafka redelivers the batch on restart, and the upsert on
# (symbol, trade_id) makes writing it twice harmless.
# At-least-once delivery + idempotent writes = effectively exactly-once rows.
#
# `xmax = 0` is only true for freshly inserted rows, so we can count how many messages
# were redeliveries that the upsert absorbed.
UPSERT_SQL = """
    INSERT INTO trades (symbol, trade_id, trade_time, price, quantity, price_z, volume_z, is_anomaly, anomaly_type,
                        size_ratio, received_at, stored_at)
    VALUES %s
    ON CONFLICT (symbol, trade_id) DO UPDATE SET
        trade_time = EXCLUDED.trade_time,
        price = EXCLUDED.price,
        quantity = EXCLUDED.quantity,
        price_z = EXCLUDED.price_z,
        volume_z = EXCLUDED.volume_z,
        is_anomaly = EXCLUDED.is_anomaly,
        anomaly_type = EXCLUDED.anomaly_type,
        size_ratio = EXCLUDED.size_ratio,
        -- keep the FIRST receive/store time, so a redelivery doesn't distort latency numbers
        received_at = COALESCE(trades.received_at, EXCLUDED.received_at),
        stored_at = COALESCE(trades.stored_at, EXCLUDED.stored_at)
    RETURNING (xmax = 0) AS inserted
"""


def consumer_config():
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        # THE FIX: no committing offsets on a timer. We commit them ourselves, after the write.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    if GROUP_INSTANCE_ID:
        config["group.instance.id"] = GROUP_INSTANCE_ID
    return config


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def to_millis_datetime(ms):
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def to_row(r, stored_at=None):
    trade_time = to_millis_datetime(r["trade_time"])
    # Decimal(str(...)) keeps the price exactly as sent (e.g. 65000.01, not 65000.0100000000020)
    return (r["symbol"], int(r["trade_id"]), trade_time, Decimal(str(r["price"])), Decimal(str(r["quantity"])),
            r["price_z"], r.get("volume_z"), r["is_anomaly"], r["anomaly_type"],
            r.get("size_ratio"), to_millis_datetime(r.get("received_at")), stored_at)


def dedupe_rows(rows):
    # Postgres refuses an upsert that touches the same row twice in one statement, so if the
    # same trade shows up twice in a batch we keep only the last copy
    latest = {}
    for row in rows:
        latest[(row[0], row[1])] = row
    return list(latest.values())


def write_batch(conn, rows):
    """Upsert all rows in one transaction: either every row is written or none is.
    Returns (inserted, updated)."""
    if not rows:
        return 0, 0
    with conn:  # commits on success, rolls back on any exception
        with conn.cursor() as cur:
            results = psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=len(rows), fetch=True)
    inserted = sum(1 for (was_inserted,) in results if was_inserted)
    return inserted, len(results) - inserted


def write_with_retry(conn, rows, reconnect=None, sleep=None):
    """Keep retrying the SAME batch until Postgres takes it. Returns (conn, inserted, updated);
    conn is a fresh connection if the old one broke."""
    reconnect = reconnect or connect_postgres
    sleep = sleep or time.sleep
    backoff = 1
    while True:
        try:
            inserted, updated = write_batch(conn, rows)
            return conn, inserted, updated
        except psycopg2.Error as e:
            log.error(f"Postgres write failed ({e}). Retrying the same batch in {backoff}s")
            sleep(backoff)
            backoff = min(backoff * 2, MAX_RETRY_BACKOFF_SECONDS)
            try:
                conn.close()
                conn = reconnect()
            except psycopg2.Error as e2:
                log.error(f"Reconnect failed: {e2}")


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


def process_batch(consumer, conn, messages, producer):
    """Write one batch to Postgres, then commit its offsets. Returns the (possibly new) connection.
    `producer` is only used to send unreadable messages to the dead-letter topic."""
    global write_count
    valid = []
    rows = []
    received_at = []
    dead_letters = 0
    stored_at = datetime.now(timezone.utc)
    for msg in messages:
        if msg.error():
            log.warning(f"Consumer error: {msg.error()}")
            continue
        valid.append(msg)
        try:
            r = json.loads(msg.value().decode("utf-8"))
            rows.append(to_row(r, stored_at))
            if r.get("received_at") is not None:
                received_at.append(r["received_at"])
        except (ValueError, KeyError, TypeError, ArithmeticError) as e:
            # a message that can never be parsed would block the partition forever, so park it
            # on the dead-letter topic (bytes unchanged) and move past it
            log.error(f"Malformed message at offset {msg.offset()}, sent to '{DEAD_LETTER_TOPIC}': {e}")
            send_to_dead_letter(producer, DEAD_LETTER_TOPIC, msg, e)
            dead_letters += 1

    if not valid:
        return conn

    if dead_letters and producer.flush(30) > 0:
        # the dead letters must be safely in Kafka before we commit past them
        raise RuntimeError("Could not deliver dead letters to Kafka; stopping without committing")

    rows = dedupe_rows(rows)
    log.debug(f"batch received messages={len(valid)} rows={len(rows)}")
    conn, inserted, updated = write_with_retry(conn, rows)
    log.debug(f"batch written inserted={inserted} updated={updated}")

    # latency = time from OUR OWN receipt of the trade to this write — same clock on both ends,
    # avoids the clock-skew issue from comparing against Binance's server timestamp
    now_ms = time.time() * 1000
    latencies_ms.extend(now_ms - t for t in received_at)
    write_count += len(rows)

    try:
        consumer.commit(offsets=next_offsets(valid), asynchronous=False)
        log.debug("offsets committed")
    except KafkaException as e:
        # e.g. a rebalance moved the partition. The rows are already saved, so the
        # redelivered copies just become no-op upserts.
        log.warning(f"Offset commit failed, batch will be redelivered: {e}")
    return conn


def main():
    consumer = Consumer(consumer_config())
    consumer.subscribe([TOPIC])
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})  # dead letters only
    conn = connect_postgres()

    log.info(f"Writing '{TOPIC}' to Postgres in batches of up to {BATCH_SIZE}...")

    try:
        while True:
            messages = consume_batch(consumer, BATCH_SIZE, BATCH_TIMEOUT_SECONDS)
            if messages:
                conn = process_batch(consumer, conn, messages, producer)
            maybe_log_metrics()

    except KeyboardInterrupt:
        # Ctrl+C mid-write rolls the transaction back and skips the commit, so the batch
        # is simply redelivered next time
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
