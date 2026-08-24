import asyncio
import json

from confluent_kafka import Producer
import websockets

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "crypto-trades"
SYMBOL = "btcusdt"
URL = f"wss://stream.binance.us:9443/ws/{SYMBOL}@trade"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
def delivery_report(err,msg):
    if err:
        print(f"Deliver failed: {err}")

    
async def stream_trades():
    async with websockets.connect(URL) as ws:
        print(f"Connected to Binance stream for {SYMBOL}, publishing to Kafka topic '{TOPIC}'")
        try:
            async for message in ws:
                trade = json.loads(message)
                producer.produce(
                    TOPIC,
                    value=json.dumps(trade).encode('utf-8'),
                    callback=delivery_report,
                )
                producer.poll(0)    # triggers delivery callbacks, non-blocking
        finally:
            producer.flush()  #anything queued gets sent before exit


if __name__ == "__main__":
    asyncio.run(stream_trades())