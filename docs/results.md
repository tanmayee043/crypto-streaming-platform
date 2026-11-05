# Results

All numbers come from test runs on a Windows laptop (Kafka and Postgres in Docker Desktop) or in CI; none come from production.

## Delivery guarantee

**Once a valid trade is in Kafka, it is stored exactly once and scored as if nothing had crashed, even if either consumer dies at any moment.** Both consumers use at-least-once delivery plus idempotent writes:

1. Read a batch from Kafka.
2. Do the work and make it durable: the detector publishes its results and waits for Kafka to acknowledge them; the storage consumer upserts the batch into Postgres in one transaction (`ON CONFLICT (symbol, trade_id, trade_time) DO UPDATE`).
3. Only then commit the Kafka offsets.

A crash before step 3 means the batch is processed again, and the upsert turns repeats into no-op updates. The detector also rebuilds each symbol's window from Kafka when it starts, so a reprocessed trade gets the score it got the first time.

Two crash tests hard-kill the real processes at random moments while trades are flowing, then compare Postgres with every trade sent:

| Test | Kills | Trades | Rows stored | Missing | Duplicates | Wrong scores |
|---|---|---|---|---|---|---|
| [Storage consumer](../scripts/crash_test.py) (synthetic, 2,000/s) | 100 (86 mid-write) | 162,969 | 162,969 | **0** | **0** | n/a |
| [Anomaly detector](../scripts/detector_crash_test.py) (real trades, 500/s) | 100 (62 mid-batch) | 60,000 | 60,000 | **0** | **0** | **0** |

CI repeats shorter versions (20 and 10 kills) on every push and pull request.

**What this doesn't cover:** the tests kill the consumers only. Trades the producer never receives, for example during a network drop, are missed; the gap checker reports them but doesn't repair them yet. Malformed messages go to dead-letter topics instead of Postgres. Kafka runs as a single broker, so losing its disk loses what it holds.

**The bug this fixed:** both consumers originally relied on Kafka's auto-commit, which commits offsets on a timer whether or not the work succeeded. In a one-off test (synthetic trades, 100/s for 30 s, one Postgres restart; not a script in this repo), the original storage consumer lost 2,066 of 3,070 trades (67%) and the fixed one lost none.

## Anomaly model

Each trade is compared with the previous 100 trades of the same symbol, never including itself:

- **Price anomaly:** the log return `ln(price / previous price)` is more than 6 robust standard deviations from the window's median.
- **Volume anomaly:** the trade is at least 50× the median trade size of the window.

Evaluated with [`evaluate_model.py`](../scripts/evaluate_model.py) on real Binance.US trades (BTC, ETH, SOL), with 1,200 clearly abnormal trades planted per file:

| Data | Model | Trades flagged | Price spikes caught | Size spikes caught |
|---|---|---|---|---|
| 60,000 recent trades | v1: raw values, mean/stdev, 30 trades | 4.63% | 72.2% | 94.7% |
| | **v2: current** | **2.89%** | **95.2%** | **97.0%** |
| 60,000 older trades (hold-out) | v1 | 4.31% | 80.0% | 94.2% |
| | **v2** | **1.55%** | **90.7%** | **97.0%** |

Real trades have no anomaly labels, so these numbers aren't precision or recall: the flag rate shows how often the model alerts, and detection is measured on planted spikes only. Thresholds were tuned on the recent set; the older set wasn't used for tuning.

Replaying the 10 Oct 2025 crash (10,507 real trades) takes about 22 seconds at 1000× and flags 735 trades (7.0%).

## Completeness

Binance trade IDs are sequential per symbol. That was checked first: 120,000 trades over REST (BTC, ETH, SOL) and a 20-minute WebSocket recording of 6 symbols (477 trades) had no gaps. So a missing ID in the database points to a trade the pipeline lost. `gap_checker.py` records each missing range in `trade_gaps`, and `GET /completeness` reports the % captured per symbol. It compares IDs between the first and last trade stored in its lookback window (24 hours by default), so it can't see trades missed outside them.

There is no long-term completeness figure yet. The target is 99.9% of trades captured over 30 days, which can only be reported after the system has really run that long.

## Performance

[`load_test.py`](../scripts/load_test.py) replays recorded real trades through the real detector and storage consumer and measures latency from receipt to Postgres, per trade, on one clock. Speed varies between runs on this laptop.

| Load | Throughput | Latency p50 | Latency p95 | Latency p99 |
|---|---|---|---|---|
| Live feed, 4 symbols, 5 minutes | ~0.3 trades/s (75 trades) | | 48 ms | |
| Steady 1,000 trades/s (4 runs: 20,000 or 60,000 trades) | ~1,000 trades/s | 14–199 ms | 27–254 ms | 246–431 ms |
| As fast as possible (2 runs: 60,000 trades) | **5,812 and 5,851 trades/s** | ~5 s | ~8.7 s | ~9 s |

- The first two rows are inside the 2-second p95 target. The live run had too few trades to fill the anomaly windows, so it shows pipeline latency without scoring.
- The last row is a burst of about 10 seconds, not long-running throughput; its latency is mostly the backlog, because all the trades are sent in about a second.
- At 1,000 trades/s, p50 was either about 15 ms or about 200 ms between runs; the cause is still open.
- **Cold start:** after a brand-new stack starts, trades took up to about 108 s to be stored for the first 2–3 minutes, then settled at 100–200 ms. This was seen in two live runs. The logs point to Kafka's first-time setup (`Coordinator load in progress`), but that isn't confirmed. The rows above weren't affected.

## Reproducing

The evaluation and load tests use recorded trades, which aren't in the repo. New recordings will give somewhat different numbers.

```bash
python scripts/record_trades.py history --symbols BTCUSDT,ETHUSDT,SOLUSDT --count 20000 --out data/history-60k.jsonl
python scripts/crash_test.py --kills 100
python scripts/detector_crash_test.py --kills 100
python scripts/evaluate_model.py data/history-60k.jsonl
python scripts/load_test.py --rate 1000
python scripts/replay.py data/history-60k.jsonl --speed 100
```

For a hold-out file, add `--skip N` to the recording command to go further back. These need `docker compose up -d --wait` first.
