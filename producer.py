import asyncio
import json

from confluent_kafka import Producer
import websockets

import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "crypto-trades"
SYMBOL = "btcusdt"
URL = f"wss://stream.binance.us:9443/ws/{SYMBOL}@trade"

MAX_BACKOFF_SECONDS = 60

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
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
        print(f"Deliver failed: {err}")

    
async def stream_trades():
    global trade_count
    async with websockets.connect(URL) as ws:
        log.info(f"Connected to Binance stream for {SYMBOL}, publishing to Kafka topic '{TOPIC}'")
        async for message in ws:
            trade = json.loads(message)
            trade["received_at"] = time.time() * 1000  # our clock time, ms — added for latency measurement
            producer.produce(
                TOPIC,
                value=json.dumps(trade).encode('utf-8'),
                callback=delivery_report,
            )
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
            log.warning(f"Connection lost ({e}). Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)



if __name__ == "__main__":
    try:
        asyncio.run(run_with_reconnect())
    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        producer.flush()  #anything queued gets sent before exit