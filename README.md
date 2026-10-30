# Crypto Market Streaming & Anomaly Detection Platform

[![CI](https://github.com/tanmayee043/crypto-streaming-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/tanmayee043/crypto-streaming-platform/actions/workflows/ci.yml)

A real-time pipeline that streams live crypto trades from Binance.US through Kafka, flags unusual price moves and unusually large trades per symbol, stores every trade in PostgreSQL exactly once, and measures its own completeness against the exchange's trade IDs.

**[Demo video coming soon]**

## Highlights

- **No lost or duplicated trades, proven end to end:** the storage consumer and the anomaly detector were each hard-killed 100 times while trades were flowing. 0 missing, 0 duplicates, and every anomaly score matched an uninterrupted run.
- **Fewer false alarms, more real anomalies caught:** on 60,000 real trades the new model flags 2.9% of trades instead of 4.6%, and catches 95% of planted price spikes instead of 72%. The gains hold on a separate 60,000-trade hold-out set.
- **Completeness you can measure:** Binance trade IDs are sequential per symbol, so a gap checker reports exactly which trades are missing and the % captured.
- **Found and fixed silent data loss:** the original storage consumer lost 67% of trades during a single Postgres restart. Now 0.
- **Replayable:** recorded real trades can be replayed at 10×/100×/1000× or at a fixed rate for repeatable tests and load tests (~3,400 trades/s on a laptop).

## Architecture

```
Binance.US WebSocket (live trades, several symbols)
        │
        ▼
producer.py ──► Kafka: crypto-trades   (key = symbol, 3 partitions)
                        │
                        ▼
              anomaly_detector.py   per-symbol windows, rebuilt from Kafka on restart;
                        │           manual offset commits after results are published
                        ▼
                Kafka: trade-analytics (key = symbol)        bad messages ──► *-dlq topics
                        │
                        ▼
              storage_consumer.py   batched upserts, manual offset commits
                        │
                        ▼
                   PostgreSQL ──► FastAPI (main.py) ──► live dashboard (Chart.js)
                        ▲
                 gap_checker.py     missing trade IDs → trade_gaps table, completeness %
```

## Delivery guarantee

**Every trade the producer publishes is stored exactly once and scored exactly as if nothing had crashed.** Both consumers use at-least-once delivery plus idempotent writes:

1. Read a batch from Kafka.
2. Do the work and make it durable: the detector publishes its results and waits for Kafka to acknowledge them; the storage consumer upserts the batch into Postgres in one transaction (`ON CONFLICT (symbol, trade_id) DO UPDATE`).
3. Only then commit the Kafka offsets.

A crash before step 3 means the batch is processed again, and the upsert turns repeats into no-op updates. The detector also **rebuilds each symbol's window** from the last messages in Kafka when it starts, so a reprocessed trade gets exactly the score it got the first time, and there is no cold-start blind spot.

**Proof:** two crash tests hard-kill the real processes at random moments while trades are flowing, then compare Postgres with every trade sent.

| Test | Kills | Trades | Rows stored | Missing | Duplicates | Wrong scores |
|---|---|---|---|---|---|---|
| [Storage consumer](scripts/crash_test.py) (synthetic, 2,000/s) | 100 (52 mid-write) | 126,123 | 126,123 | **0** | **0** | n/a |
| [Anomaly detector](scripts/detector_crash_test.py) (real trades, 500/s) | 100 (95 mid-batch) | 60,000 | 60,000 | **0** | **0** | **0** |

**The bug this fixed:** both consumers originally relied on Kafka's auto-commit, which commits offsets on a timer whether or not the work succeeded. In a test with one Postgres restart, the original storage consumer lost 2,066 of 3,070 trades; the fixed one lost none. The original detector also never published the first 29 trades after each start, so they never reached the database.

## Anomaly model

Each trade is compared with the previous 100 trades **of the same symbol**, never including itself:

- **Price anomaly:** the log return `ln(price / previous price)` is more than 6 robust standard deviations from the window's median. Log returns make a move mean the same at any price level; the median, with a scale floor that resists lumpy trade data, keeps one big move from hiding the next.
- **Volume anomaly:** the trade is at least 50× the median trade size of the window.

Evaluated with [`scripts/evaluate_model.py`](scripts/evaluate_model.py) on real Binance.US trades (BTC, ETH, SOL), with 1,200 clearly abnormal trades planted to measure detection:

| Data | Model | Trades flagged | Price spikes caught | Size spikes caught |
|---|---|---|---|---|
| 60,000 recent trades | v1: raw values, mean/stdev, 30 trades | 4.63% | 72.2% | 94.7% |
| | **v2: current** | **2.89%** | **95.2%** | **97.0%** |
| 60,000 older trades (hold-out) | v1 | 4.31% | 80.0% | 94.2% |
| | **v2** | **1.55%** | **90.7%** | **97.0%** |

## Completeness

Binance trade IDs are sequential per symbol. That was checked first: 120,000 trades over REST and a 20-minute live WebSocket recording of 6 symbols had no gaps. So any missing ID in the database is a trade the pipeline lost, at any stage. `gap_checker.py` records each missing range in `trade_gaps` (and marks it resolved once filled), and `GET /completeness` reports the % captured per symbol.

## Performance

[`scripts/load_test.py`](scripts/load_test.py) replays 60,000 real trades through the detector and storage consumer and measures latency from receipt to Postgres (one clock, no skew):

| Load | Throughput | Latency p50 | Latency p95 |
|---|---|---|---|
| Live feed (~1 trade/s, 30-min run, before Week 2) | ~1.15 trades/s | 32 ms | 40 ms |
| Steady 1,000 trades/s (5 runs) | 1,000 trades/s | 15–200 ms | 75–280 ms |
| As fast as possible | ~3,400 trades/s | backlog | backlog |

The bottleneck at full speed is the detector (two medians over 100 values per trade in pure Python); more partitions with more detector instances would scale it. Why the 1,000 trades/s latency varies between runs is still being investigated.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python, `websockets`, Binance.US public API |
| Messaging | Apache Kafka 4.3 (KRaft), `confluent-kafka`, dead-letter topics |
| Stream processing | Plain Python (per-symbol windows, robust z-score) |
| Storage | PostgreSQL 16 (`NUMERIC` prices), `psycopg2` |
| API and dashboard | FastAPI, HTML/JS with Chart.js |
| Testing and CI | pytest, Testcontainers, ruff, GitHub Actions |
| Infrastructure | Docker Compose (pinned image versions) |

## Getting started

**Prerequisites:** Python 3.11+, Docker Desktop, Git.

```bash
git clone https://github.com/tanmayee043/crypto-streaming-platform.git
cd crypto-streaming-platform
python -m venv venv
venv\Scripts\activate              # Mac/Linux: source venv/bin/activate
pip install -r requirements.txt
copy .env.example .env             # Mac/Linux: cp; then set your own password
docker compose up -d --wait
```

A fresh Postgres volume gets the full schema from `schema.sql`. An existing database needs the files in `migrations/` applied in order, e.g. `Get-Content migrations\001_numeric_prices_and_timestamps.sql | docker exec -i postgres psql -U crypto -d crypto_db` (PowerShell).

Create the Kafka topics once (3 partitions, keyed by symbol):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic crypto-trades --partitions 3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic trade-analytics --partitions 3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic crypto-trades-dlq --partitions 3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic trade-analytics-dlq --partitions 3
```

Run each service in its own terminal:

```bash
python producer.py
python anomaly_detector.py
python storage_consumer.py
python gap_checker.py
uvicorn main:app --reload
```

Then open the dashboard at `http://localhost:8000/dashboard/`, completeness at `http://localhost:8000/completeness`, the API docs at `http://localhost:8000/docs`, or Kafka UI at `http://localhost:8080`.

### Replaying recorded trades

```bash
python scripts/record_trades.py history --symbols BTCUSDT,ETHUSDT,SOLUSDT --count 20000 --out data/history.jsonl
python scripts/replay.py data/history.jsonl --speed 100        # 100x real time; also --rate 1000, or --speed 0 for max
```

### Configuration

All settings come from environment variables (or `.env`). See [`.env.example`](.env.example) for the full list.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Postgres address |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | (required) | Postgres credentials |
| `SYMBOLS` | `btcusdt` | Comma-separated pairs to stream |
| `WINDOW_SIZE` | `100` | Trades per symbol in the detector's window |
| `PRICE_Z_THRESHOLD` / `SIZE_RATIO_THRESHOLD` | `6.0` / `50` | Anomaly thresholds |
| `STORAGE_BATCH_SIZE` | `500` | Max trades per database transaction |

## Testing

```bash
pip install -r requirements-dev.txt
ruff check .                                      # lint
pytest                                            # unit + integration tests (integration needs Docker)
python scripts/crash_test.py --kills 100          # storage consumer crash test (needs docker compose up)
python scripts/detector_crash_test.py --kills 100 # detector crash test (needs data/history-60k.jsonl)
python scripts/evaluate_model.py data/history-60k.jsonl
python scripts/load_test.py --rate 1000
```

- **Unit tests** cover the anomaly model (log returns, robust z-score, size ratio, current trade excluded), per-symbol state, both consumers' "make it durable, then commit" order, dead-lettering, and the replay and recording helpers.
- **Integration tests** run against a throwaway Postgres container (Testcontainers): exact `NUMERIC` prices, idempotent upserts, all-or-nothing batches, reconnects, and the gap checker.
- **CI** runs lint, the tests, a 20-kill storage crash test and a 10-kill detector crash test on every push and pull request.

## Project structure

```
producer.py              Binance.US WebSocket → Kafka, keyed by symbol, reconnects with backoff
anomaly_detector.py      crypto-trades → per-symbol anomaly model → trade-analytics
storage_consumer.py      trade-analytics → Postgres (batched upserts, manual offset commits)
gap_checker.py           missing trade IDs and completeness %
kafka_helpers.py         offset, batching and dead-letter helpers shared by both consumers
main.py                  FastAPI: /trades, /anomalies, /anomalies/count, /completeness, /dashboard
schema.sql, migrations/  database schema and upgrades for existing databases
static/index.html        live dashboard
scripts/                 recording, replay, model evaluation, crash tests, load test
tests/                   unit and integration tests, sample trade file
```

## Design decisions and limitations

- **Hash partitioning with few symbols is uneven.** With `btcusdt,ethusdt,solusdt,xrpusdt`, BTC and ETH share partition 0, so one detector instance per partition wouldn't be perfectly balanced.
- **Window rebuild reads the last 2,000 messages per partition.** That is exact while each symbol had at least 101 trades in those messages; a very rare symbol sharing a partition with busy ones could restart with a partly filled window.
- **Masking in degenerate windows.** When more than half of a window is identical, one huge outlier can still raise the scale enough to hide the next anomaly.
- **Thresholds were tuned on historical data** (checked on a hold-out set). They should be re-checked periodically as market conditions change.
- **Dead letters are kept but not yet shown** anywhere. They're readable in Kafka UI.
- **Gaps are reported, not yet repaired.** Backfilling missing trades from the exchange's REST API is planned for the Lakehouse project.
- **Kafka data is not on a named volume**, so recreating the Kafka container wipes the topics.
- **Polling dashboard** (every 3 s), showing one symbol (`BTCUSDT`).

## Roadmap

- Server-Sent Events dashboard with anomaly detail drawer, replay mode and pipeline health strip
- Prometheus metrics and Grafana (per-stage latency, consumer lag, completeness)
- 24/7 deployment with external uptime monitoring
- Automatic backfill of gaps from the exchange's REST API
