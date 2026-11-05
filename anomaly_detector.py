import json
import math
import statistics
from collections import deque
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from prometheus_client import Counter, Histogram, start_http_server

import logging
import time

import os
from dotenv import load_dotenv

load_dotenv()

from kafka_helpers import consume_batch, next_offsets, send_to_dead_letter  # noqa: E402
from service_setup import settings, setup_logging, stop_after_batch_on_sigterm, stop_requested  # noqa: E402

setup_logging()
log = logging.getLogger("anomaly_detector")

KAFKA_BOOTSTRAP_SERVERS = settings.kafka_bootstrap_servers
INPUT_TOPIC = os.getenv("TRADES_TOPIC", "crypto-trades")
OUTPUT_TOPIC = os.getenv("ANALYTICS_TOPIC", "trade-analytics")
DEAD_LETTER_TOPIC = os.getenv("TRADES_DLQ_TOPIC", "crypto-trades-dlq")
GROUP_ID = os.getenv("DETECTOR_GROUP_ID", "crypto-anomaly-detector")
# optional Kafka "static membership": a restarted detector gets its partitions back at once
GROUP_INSTANCE_ID = os.getenv("DETECTOR_GROUP_INSTANCE_ID")

BATCH_SIZE = settings.detector_batch_size
BATCH_TIMEOUT_SECONDS = 1.0
FLUSH_TIMEOUT_SECONDS = 30

WINDOW_SIZE = settings.window_size
# Thresholds chosen with scripts/evaluate_model.py on 60k real Binance.US trades (see README)
PRICE_Z_THRESHOLD = settings.price_z_threshold
SIZE_RATIO_THRESHOLD = settings.size_ratio_threshold
# how many messages per partition to re-read on startup to rebuild the windows
REBUILD_LOOKBACK = settings.rebuild_lookback

METRICS_INTERVAL_SECONDS = settings.metrics_interval_seconds
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))  # 0 turns the /metrics endpoint off
# localhost only: Prometheus runs on this machine, and nothing else should reach the metrics
METRICS_HOST = os.getenv("METRICS_HOST", "127.0.0.1")

# --- Prometheus metrics (scraped from http://localhost:8002/metrics) ---
LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)  # seconds
TRADES_SCORED = Counter("detector_trades_total", "Trades processed", ["symbol"])
ANOMALIES = Counter("detector_anomalies_total", "Anomalies flagged", ["symbol", "type"])
DEAD_LETTERS = Counter("detector_dead_letters_total", "Trades sent to the dead-letter topic")
REBUILT_TRADES = Counter("detector_rebuild_trades_total", "Trades re-read from Kafka to rebuild windows")
BATCH_SECONDS = Histogram(
    "detector_batch_seconds", "Time to score, publish and commit one batch", buckets=LATENCY_BUCKETS
)
# stage 1 of the end-to-end latency: producer received the trade -> its result is safely in Kafka
STAGE_LATENCY = Histogram(
    "detector_stage_latency_seconds",
    "Producer receipt to detector result acknowledged by Kafka",
    buckets=LATENCY_BUCKETS,
)

# --- the anomaly model ---
#
# For each symbol we keep the last WINDOW_SIZE log returns, ln(price / previous price), and
# the last WINDOW_SIZE trade sizes. A new trade is scored against the window BEFORE it is
# added, so a spike can't hide by inflating its own baseline.
#
# Price: a robust z-score of the log return.
# - Log returns instead of raw prices: a $50 move means the same at $30k or $100k BTC, and
#   returns don't trend the way prices do.
# - Median/MAD instead of mean/stdev: one big move drags the mean and inflates the stdev,
#   which hides the next anomaly. The median and MAD barely move.
# - Scale floor: real trade data is lumpy (one order filling several resting orders gives
#   runs of identical prices), which can shrink the MAD to almost nothing and make ordinary
#   moves look extreme. Using the larger of the MAD and the mean absolute deviation stops that.
#
# Size: how many times bigger the trade is than the median trade of the window. Trade sizes
# are so skewed (the 99th percentile is ~50x the median) that a ratio to the median is both
# more reliable and easier to explain than a z-score.

MAD_TO_STDEV = 1 / 0.6745  # makes the MAD comparable to a standard deviation
MEAN_AD_TO_STDEV = 1.2533  # same, for the mean absolute deviation


def robust_z(value, values):
    """How far `value` is from the window's median, in robust standard deviations."""
    median = statistics.median(values)
    deviations = [abs(v - median) for v in values]
    scale = max(
        MAD_TO_STDEV * statistics.median(deviations),
        # fsum, not statistics.mean: mean does exact-fraction maths and was the slowest
        # part of the whole detector (see the profile in the guide)
        MEAN_AD_TO_STDEV * math.fsum(deviations) / len(deviations),
    )
    if scale == 0:
        return 0.0
    return (value - median) / scale


def new_window():
    return {
        "last_price": None,
        "returns": deque(maxlen=WINDOW_SIZE),
        "sizes": deque(maxlen=WINDOW_SIZE),
    }


def score_trade(window, price, quantity):
    """Score a trade against its symbol's window, then add it to the window.
    Returns (price_z, size_ratio); each is None until there are WINDOW_SIZE earlier values."""
    if price <= 0 or quantity <= 0:
        raise ValueError(f"price and quantity must be positive, got {price} / {quantity}")
    log_return = math.log(price / window["last_price"]) if window["last_price"] else None

    price_z = None
    if log_return is not None and len(window["returns"]) == WINDOW_SIZE:
        price_z = robust_z(log_return, window["returns"])
    size_ratio = None
    if len(window["sizes"]) == WINDOW_SIZE:
        size_ratio = quantity / statistics.median(window["sizes"])

    if log_return is not None:
        window["returns"].append(log_return)
    window["sizes"].append(quantity)
    window["last_price"] = price
    return price_z, size_ratio


def classify_anomaly(price_z, size_ratio, price_threshold=PRICE_Z_THRESHOLD, size_threshold=SIZE_RATIO_THRESHOLD):
    price_hit = price_z is not None and abs(price_z) > price_threshold
    size_hit = size_ratio is not None and size_ratio >= size_threshold
    if price_hit and size_hit:
        return "both"
    elif price_hit:
        return "price"
    elif size_hit:
        return "volume"
    return None


# --- per-symbol state ---
# Trades are keyed by symbol, so every trade of a symbol lands on the same partition, in
# order. That lets each symbol keep its own window, and lets us rebuild it from Kafka.
windows: dict = {}  # symbol -> window
partition_of: dict = {}  # symbol -> partition it arrives on


def update_window(trade, partition):
    symbol = trade["s"]
    if symbol not in windows:
        windows[symbol] = new_window()
        partition_of[symbol] = partition
    return score_trade(windows[symbol], float(trade["p"]), float(trade["q"]))


def build_result(trade, price_z, size_ratio, anomaly_type):
    return {
        "symbol": trade["s"],
        "trade_id": trade["t"],
        "trade_time": trade["T"],  # Binance sends epoch milliseconds
        "received_at": trade.get("received_at"),
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "price_z": price_z,  # None while the symbol's window is warming up
        "size_ratio": size_ratio,  # trade size / median size of the window
        "volume_z": None,  # v1 field, no longer computed
        "is_anomaly": anomaly_type is not None,
        "anomaly_type": anomaly_type,  # "price" | "volume" | "both" | None
    }


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
            f"Processed {processed_count} trades in {elapsed:.0f}s, {anomaly_count} anomalies flagged ({rate:.1f}%)"
        )
        processed_count = 0
        anomaly_count = 0
        last_metrics_log = now


# --- recovery: rebuild the windows from Kafka on startup ---


def rebuild_windows(committed):
    """Re-read the last REBUILD_LOOKBACK messages before each partition's start position and
    feed them through the model without publishing anything. Afterwards each symbol's window
    is exactly what it was before the restart, so there is no cold-start blind spot and a
    reprocessed trade gets the same scores it got the first time."""
    reader = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"{GROUP_ID}-rebuild",  # never commits anything
            "enable.auto.commit": False,
        }
    )
    try:
        for tp in committed:
            for symbol in [s for s, p in partition_of.items() if p == tp.partition]:
                del windows[symbol], partition_of[symbol]

            low, high = reader.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)
            start_at = max(tp.offset, low)  # no commit yet (offset < 0): start at the beginning
            tp.offset = start_at
            read_from = max(low, start_at - REBUILD_LOOKBACK)
            if read_from >= start_at:
                continue

            reader.assign([TopicPartition(tp.topic, tp.partition, read_from)])
            replayed = 0
            done = False
            while not done:
                messages = reader.consume(num_messages=BATCH_SIZE, timeout=5)
                if not messages:
                    log.warning(f"Rebuild of partition {tp.partition} stopped early at {replayed} trades")
                    break
                for msg in messages:
                    if msg.error():
                        continue
                    if msg.offset() >= start_at:
                        done = True
                        break
                    try:
                        update_window(json.loads(msg.value() or b""), tp.partition)
                        replayed += 1
                        REBUILT_TRADES.inc()
                    except (ValueError, KeyError, TypeError):
                        pass  # it went to the dead-letter topic the first time round
                    if msg.offset() == start_at - 1:
                        done = True
            log.info(
                f"Rebuilt windows for partition {tp.partition} from {replayed} trades "
                f"(offsets {read_from}-{start_at - 1})"
            )
    finally:
        reader.close()


def on_assign(consumer, partitions):
    committed = consumer.committed(partitions, timeout=10)
    rebuild_windows(committed)
    consumer.assign(committed)  # start each partition exactly where the rebuild stopped


def on_revoke(consumer, partitions):
    revoked = {tp.partition for tp in partitions}
    for symbol in [s for s, p in partition_of.items() if p in revoked]:
        del windows[symbol], partition_of[symbol]


# --- the processing loop ---

delivery_errors: list = []


def on_delivery(err, msg):
    if err:
        delivery_errors.append(err)
        log.error(f"Delivery failed: {err}")


def process_batch(consumer, producer, messages):
    """Score a batch and publish the results, then commit. Same rule as the storage consumer:
    offsets are only committed once every result is safely in Kafka."""
    global processed_count, anomaly_count
    log.debug(f"batch received messages={len(messages)}")
    batch_started = time.time()
    received_at = []
    valid = []
    for msg in messages:
        if msg.error():
            log.warning(f"Consumer error: {msg.error()}")
            continue
        valid.append(msg)
        try:
            trade = json.loads(msg.value().decode("utf-8"))
            price_z, size_ratio = update_window(trade, msg.partition())
            anomaly_type = classify_anomaly(price_z, size_ratio)
            result = build_result(trade, price_z, size_ratio, anomaly_type)
        except (ValueError, KeyError, TypeError) as e:
            log.error(f"Bad trade at offset {msg.offset()}, sent to '{DEAD_LETTER_TOPIC}': {e}")
            send_to_dead_letter(producer, DEAD_LETTER_TOPIC, msg, e)
            DEAD_LETTERS.inc()
            continue

        TRADES_SCORED.labels(trade["s"]).inc()
        if trade.get("received_at") is not None:
            received_at.append(trade["received_at"])

        if price_z is not None or size_ratio is not None:
            processed_count += 1
        if anomaly_type:
            anomaly_count += 1
            ANOMALIES.labels(trade["s"], anomaly_type).inc()
            log.info(
                f"🚨 {anomaly_type.upper()} ANOMALY — {trade['s']} price={result['price']}, "
                f"qty={result['quantity']}, price_z={price_z}, size_ratio={size_ratio}"
            )

        # Publish the result downstream for storage_consumer.py to persist, keyed by symbol
        producer.produce(
            OUTPUT_TOPIC,
            key=trade["s"].encode("utf-8"),
            value=json.dumps(result).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    if not valid:
        return

    # wait until Kafka has acknowledged every result (and dead letter) from this batch
    if producer.flush(FLUSH_TIMEOUT_SECONDS) > 0 or delivery_errors:
        # Don't commit. Exiting means the windows are rebuilt and the batch is reprocessed
        # on restart, instead of carrying on with a window that saw trades we never published.
        raise RuntimeError("Results were not delivered to Kafka; stopping without committing")

    log.debug(f"batch written results={len(valid)}")
    acked_ms = time.time() * 1000
    for t in received_at:
        STAGE_LATENCY.observe(max(acked_ms - t, 0) / 1000)

    try:
        consumer.commit(offsets=next_offsets(valid), asynchronous=False)
        log.debug("offsets committed")
    except KafkaException as e:
        log.warning(f"Offset commit failed, batch will be reprocessed (the upsert absorbs it): {e}")
    BATCH_SECONDS.observe(time.time() - batch_started)


def consumer_config():
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        # a brand-new detector processes everything still in Kafka, so nothing is skipped
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    if GROUP_INSTANCE_ID:
        config["group.instance.id"] = GROUP_INSTANCE_ID
    return config


def main():
    stop_after_batch_on_sigterm()
    if METRICS_PORT:
        start_http_server(METRICS_PORT, addr=METRICS_HOST)
    consumer = Consumer(consumer_config())
    consumer.subscribe([INPUT_TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    # idempotence: Kafka drops duplicates caused by the producer's own retries
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True})

    log.info(
        f"Watching '{INPUT_TOPIC}' — window={WINDOW_SIZE} trades per symbol, "
        f"price z > {PRICE_Z_THRESHOLD}, size >= {SIZE_RATIO_THRESHOLD}x median"
    )
    log.info(f"Publishing results to '{OUTPUT_TOPIC}'")

    try:
        while not stop_requested.is_set():
            messages = consume_batch(consumer, BATCH_SIZE, BATCH_TIMEOUT_SECONDS)
            if messages:
                process_batch(consumer, producer, messages)
            maybe_log_metrics()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush(FLUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
