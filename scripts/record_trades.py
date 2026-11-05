"""Records real Binance.US trades to a JSON-lines file that scripts/replay.py can play back.

Each line is one trade in the same shape the WebSocket sends (e, s, t, p, q, T, m), so a
recording replays through the pipeline exactly like live data.

Two ways to get trades:

    # live: listen to the WebSocket for 30 minutes
    python scripts/record_trades.py live --symbols btcusdt,ethusdt,solusdt --minutes 30 --out data/live.jsonl

    # history: page back through the public REST API (no API key needed)
    python scripts/record_trades.py history --symbols BTCUSDT,ETHUSDT --count 20000 --out data/history.jsonl
    # a time window, e.g. a volatile day (UTC)
    python scripts/record_trades.py history --symbols BTCUSDT --start 2025-10-10T19:00 \
        --end 2025-10-11T01:00 --out data/crash.jsonl
    # an older stretch, e.g. to test a model on data it wasn't tuned on
    python scripts/record_trades.py history --symbols BTCUSDT --count 20000 --skip 100000 --out data/older.jsonl

Both modes print a trade-ID check per symbol at the end: Binance trade IDs are sequential
per symbol, so any jump means a trade we never received.
"""

import argparse
from datetime import datetime, timezone
import asyncio
import json
from itertools import pairwise
import time
import urllib.request
from pathlib import Path

import websockets

WS_URL = "wss://stream.binance.us:9443/stream?streams="
REST_URL = "https://api.binance.us/api/v3/historicalTrades"
AGG_URL = "https://api.binance.us/api/v3/aggTrades"
REST_PAGE = 1000  # max trades per REST call


def check_ids(trades):
    """Per symbol: trades seen, ID range, and how many IDs in that range are missing."""
    by_symbol: dict = {}
    for t in trades:
        by_symbol.setdefault(t["s"], []).append(t["t"])
    report = {}
    for symbol, ids in sorted(by_symbol.items()):
        unique = sorted(set(ids))
        expected = unique[-1] - unique[0] + 1
        report[symbol] = {
            "trades": len(ids),
            "first_id": unique[0],
            "last_id": unique[-1],
            "missing": expected - len(unique),
            "duplicates": len(ids) - len(unique),
            "out_of_order": sum(1 for a, b in pairwise(ids) if b < a),
        }
    return report


async def record_live(symbols, minutes, out):
    url = WS_URL + "/".join(f"{s.lower()}@trade" for s in symbols)
    deadline = time.time() + minutes * 60
    trades = []
    with out.open("w") as f:
        while time.time() < deadline:
            try:
                async with websockets.connect(url) as ws:
                    print(f"Recording {', '.join(symbols)} for {minutes} min -> {out}")
                    while time.time() < deadline:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
                        except asyncio.TimeoutError:
                            break
                        trade = json.loads(message)["data"]  # combined streams wrap each message
                        f.write(json.dumps(trade) + "\n")
                        trades.append(trade)
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                print(f"Connection lost ({e}), reconnecting")
                await asyncio.sleep(1)
    return trades


def first_trade_id_at(symbol, start_ms):
    """The id of the first trade at or after `start_ms`. historicalTrades can only page by id,
    but aggTrades can search by time, and each aggregate trade records its first trade id."""
    with urllib.request.urlopen(f"{AGG_URL}?symbol={symbol}&startTime={start_ms}&limit=1", timeout=20) as r:
        return json.load(r)[0]["f"]


def fetch_history(symbols, count, out, skip=0, start_ms=None, end_ms=None):
    trades: list = []
    for symbol in symbols:
        symbol = symbol.upper()
        if start_ms is not None:
            # a time window: from the first trade at start_ms until a trade passes end_ms
            from_id, newest = first_trade_id_at(symbol, start_ms), float("inf")
        else:
            # the newest trade id, then walk backwards one page at a time
            with urllib.request.urlopen(f"{REST_URL}?symbol={symbol}&limit=1", timeout=20) as r:
                newest = json.load(r)[0]["id"]
            newest -= skip  # --skip lets you fetch an older stretch, e.g. as a hold-out set
            from_id = max(0, newest - count + 1)
        fetched = []
        while from_id <= newest:
            url = f"{REST_URL}?symbol={symbol}&limit={REST_PAGE}&fromId={from_id}"
            with urllib.request.urlopen(url, timeout=20) as r:
                page = json.load(r)
            if not page:
                break
            if end_ms is not None:
                page = [x for x in page if x["time"] <= end_ms]
                fetched.extend(page)
                if not page or page[-1]["time"] >= end_ms or len(page) < REST_PAGE:
                    break
            else:
                fetched.extend(page)
            from_id = page[-1]["id"] + 1
            time.sleep(1.5)  # stay well under the REST weight limit
        print(f"{symbol}: fetched {len(fetched)} trades")
        trades.extend(
            {
                "e": "trade",
                "s": symbol,
                "t": x["id"],
                "p": x["price"],
                "q": x["qty"],
                "T": x["time"],
                "m": x["isBuyerMaker"],
            }
            for x in fetched
        )
    trades.sort(key=lambda t: (t["T"], t["s"], t["t"]))  # interleave symbols in time order
    with out.open("w") as f:
        for t in trades:
            f.write(json.dumps(t) + "\n")
    return trades


def main():
    parser = argparse.ArgumentParser(description="Record Binance.US trades for replay")
    parser.add_argument("mode", choices=["live", "history"])
    parser.add_argument("--symbols", default="btcusdt,ethusdt,solusdt")
    parser.add_argument("--minutes", type=float, default=30, help="live mode: how long to record")
    parser.add_argument("--count", type=int, default=10000, help="history mode: trades per symbol")
    parser.add_argument("--skip", type=int, default=0, help="history mode: skip this many newest trades")
    parser.add_argument("--start", help="history mode: UTC start, e.g. 2025-10-10T20:00 (instead of --count)")
    parser.add_argument("--end", help="history mode: UTC end, e.g. 2025-10-11T00:00")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if args.mode == "live":
        trades = asyncio.run(record_live(symbols, args.minutes, args.out))
    else:

        def utc_ms(text):
            return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)

        start_ms = utc_ms(args.start) if args.start else None
        end_ms = utc_ms(args.end) if args.end else None
        trades = fetch_history(symbols, args.count, args.out, args.skip, start_ms, end_ms)

    print(f"Wrote {len(trades)} trades to {args.out}")
    for symbol, r in check_ids(trades).items():
        print(
            f"  {symbol}: {r['trades']} trades, ids {r['first_id']}..{r['last_id']}, "
            f"missing={r['missing']} duplicates={r['duplicates']} out_of_order={r['out_of_order']}"
        )


if __name__ == "__main__":
    main()
