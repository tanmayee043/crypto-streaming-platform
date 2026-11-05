# https://www.newsletter.quantreo.com/p/live-crypto-data-in-15-lines-of-python

import asyncio
import json
import websockets

SYMBOL = "btcusdt"  # this is the trading pair (Bitcoin/USDT)
URL = "wss://stream.binance.us:9443/ws/btcusdt@trade"  # this is the link for binance US service
# Use 'ethusdt' for etherium


async def stream_trades():
    async with websockets.connect(URL) as ws:
        print(f"Connected to Binance stream for {SYMBOL}")
        async for message in ws:
            trade = json.loads(message)
            price = float(trade["p"])
            quantity = float(trade["q"])
            print(f"Price: {price:.2f}  Quantity: {quantity:.5f}")


if __name__ == "__main__":
    asyncio.run(stream_trades())
