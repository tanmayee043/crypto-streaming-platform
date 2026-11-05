import asyncio
import json

from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server
import websockets

import logging
import time

import os
from dotenv import load_dotenv
load_dotenv()

from service_setup import exit_on_sigterm, settings, setup_logging  # noqa: E402

setup_logging()
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = settings.kafka_bootstrap_servers
TOPIC = os.getenv("TRADES_TOPIC", "crypto-trades")
# comma-separated, e.g. "btcusdt,ethusdt,solusdt" (SYMBOL still works for a single pair)
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", os.getenv("SYMBOL", "btcusdt")).split(",") if s.strip()]
# one "combined stream" connection carries every symbol; each message is {"stream": ..., "data": trade}
URL = "wss://stream.binance.us:9443/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

MAX_BACKOFF_SECONDS = 60
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))  # 0 turns the /metrics endpoint off
# localhost only: Prometheus runs on this machine, and nothing else should reach the metrics
METRICS_HOST = os.getenv("METRICS_HOST", "127.0.0.1")

# --- Prometheus metrics (scraped from http://localhost:8001/metrics) ---
TRADES_RECEIVED = Counter("producer_trades_total", "Trades received from Binance and sent to Kafka", ["symbol"])
RECONNECTS = Counter("producer_reconnects_total", "WebSocket reconnects")
DELIVERY_FAILURES = Counter("producer_delivery_failures_total", "Trades Kafka did not accept")

# idempotence: Kafka drops duplicates caused by the producer's own retries
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "enable.idempotence": True})
#- --- throughput metrics ---- #
trade_count = 0
last_metrics_log = time.time()
METRICS_INTERVAL_SECONDS = 60


def maybe_log_metrics():
    global trade_count, last_metrics_log
    now = time.time()
    elapsed = now - last_metrics_log
    if elapsed >= METRICS_INTERVAL_SECONDS:
        rate = trade_count / elapsed
        log.info(f"Throughput: {trade_count} trades in {elapsed:.0f}s ({rate:.2f} trades/sec)")
        trade_count = 0
        last_metrics_log = now

def delivery_report(err,msg):
    if err:
        DELIVERY_FAILURES.inc()
        print(f"Deliver failed: {err}")


def send(trade):
    # Keyed by symbol: every trade of a symbol goes to the same partition, in order, which
    # the detector relies on for its per-symbol windows
    while True:
        try:
            producer.produce(
                TOPIC,
                key=trade["s"].encode("utf-8"),
                value=json.dumps(trade).encode('utf-8'),
                callback=delivery_report,
            )
            return
        except BufferError:
            # backpressure: the local send buffer is full (Kafka slow or down), so wait for
            # some deliveries to finish instead of crashing or dropping the trade
            log.warning("Producer buffer full, waiting for Kafka to catch up")
            producer.poll(1)


async def stream_trades():
    global trade_count
    async with websockets.connect(URL) as ws:
        log.info(f"Connected to Binance stream for {', '.join(SYMBOLS)}, publishing to Kafka topic '{TOPIC}'")
        async for message in ws:
            trade = json.loads(message)["data"]
            trade["received_at"] = time.time() * 1000  # our clock time, ms — added for latency measurement
            send(trade)
            TRADES_RECEIVED.labels(trade["s"]).inc()
            producer.poll(0)    # triggers delivery callbacks, non-blocking
            trade_count+=1
            maybe_log_metrics()


async def run_with_reconnect():
    backoff = 1
    while True:
        try:
            await stream_trades()
            # if stream_trades() returns normally after running a while,
            # treat it as a disconnect and reset backoff since the connection was healthy
            backoff = 1
        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            RECONNECTS.inc()
            log.warning(f"Connection lost ({e}). Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)



if __name__ == "__main__":
    exit_on_sigterm()
    if METRICS_PORT:
        start_http_server(METRICS_PORT, addr=METRICS_HOST)
    try:
        asyncio.run(run_with_reconnect())
    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        producer.flush()  #anything queued gets sent before exit
