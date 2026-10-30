# Crypto Market Streaming & Anomaly Detection Platform

[![CI](https://github.com/tanmayee043/crypto-streaming-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/tanmayee043/crypto-streaming-platform/actions/workflows/ci.yml)

A real-time pipeline that streams live crypto trades from Binance.US through Kafka, flags unusual price and volume activity with a rolling z-score, stores every trade in PostgreSQL exactly once, and shows it all on a live dashboard.

**[Demo video coming soon]**

## Highlights

- **No lost or duplicated trades, proven:** the storage consumer was hard-killed 100 times while trades were flowing, and all 126,123 trades still ended up in Postgres, with 0 missing and 0 duplicates.
- **Found and fixed a silent data-loss bug:** the original consumer lost 67% of trades during a single Postgres restart. The fix brings that to 0.
- **Low latency:** p50 ≈ 32 ms and p95 ≈ 40 ms from receiving a trade to writing it to Postgres, on the live feed.
- **Independent services:** ingestion, detection, storage and API talk only through Kafka, so any one of them can restart without taking down the others.

## Architecture

```
Binance.US WebSocket (live trades)
        │
        ▼
producer.py ──► Kafka: crypto-trades
                        │
                        ▼
              anomaly_detector.py   (rolling 30-trade window, z-score on price and quantity)
                        │
                        ▼
                Kafka: trade-analytics
                        │
                        ▼
              storage_consumer.py   (batched upserts, manual offset commits)
                        │
                        ▼
                   PostgreSQL ──► FastAPI (main.py) ──► live dashboard (Chart.js)
```

## Delivery guarantee

**Every trade on `trade-analytics` is stored exactly once, even if the storage consumer crashes at any moment.** This uses at-least-once delivery plus idempotent writes:

1. Read a batch of up to 500 messages from Kafka.
2. Upsert the batch into Postgres in one transaction (`ON CONFLICT (symbol, trade_id) DO UPDATE`). If it fails, retry the same batch, reconnecting if needed.
3. Only then commit the Kafka offsets.

A crash before step 3 means Kafka redelivers the batch. The upsert turns those repeats into no-op updates instead of duplicate rows.

**Proof:** [`scripts/crash_test.py`](scripts/crash_test.py) streams 2,000 trades/s into Kafka and hard-kills the real consumer at random moments, then compares Postgres with every trade Kafka acknowledged.

| Hard kills | Kills mid-write | Trades sent | Rows stored | Missing | Duplicates | Redeliveries absorbed |
|---|---|---|---|---|---|---|
| 100 | 52 | 126,123 | 126,123 | **0** | **0** | 2,500 |

**The bug this fixed:** the consumer originally used Kafka's auto-commit, which commits offsets on a timer whether or not the write succeeded, and it logged and skipped failed inserts. In a test with one Postgres restart, it lost 2,066 of 3,070 trades. The fixed consumer lost none.

## Metrics (live feed)

Measured over a ~30-minute run on Binance.US BTC/USDT:

| Metric | Value |
|---|---|
| Throughput | ~1.15 trades/s sustained, ~3 trades/s peak |
| End-to-end latency (receipt → Postgres) | p50 ≈ 32 ms, p95 ≈ 40 ms |
| Anomaly rate | 91 of 1,525 trades ≈ 6.0% (threshold 3σ) |

Latency is measured on one clock (the producer stamps each trade on receipt), so clock skew against Binance's servers doesn't distort it.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python, `websockets`, Binance.US public API |
| Messaging | Apache Kafka 4.3 (KRaft, no Zookeeper), `confluent-kafka` |
| Stream processing | Plain Python (count-based sliding window, z-score) |
| Storage | PostgreSQL 16, `psycopg2` |
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

The `trades` table is created from `schema.sql` the first time the Postgres volume starts. For an existing volume, load it by hand with `Get-Content schema.sql | docker exec -i postgres psql -U crypto -d crypto_db` (PowerShell). Create the Kafka topics once:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic crypto-trades --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic trade-analytics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Run each service in its own terminal:

```bash
python producer.py
python anomaly_detector.py
python storage_consumer.py
uvicorn main:app --reload
```

Then open the dashboard at `http://localhost:8000/dashboard/`, the API docs at `http://localhost:8000/docs`, or Kafka UI at `http://localhost:8080`.

### Configuration

All settings come from environment variables (or `.env`). See [`.env.example`](.env.example) for the full list.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | Postgres address |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | (required) | Postgres credentials |
| `SYMBOL` | `btcusdt` | Trading pair to stream |
| `STORAGE_BATCH_SIZE` | `500` | Max trades per database transaction |

## Testing

```bash
pip install -r requirements-dev.txt
ruff check .                              # lint
pytest                                    # unit + integration tests (integration needs Docker)
python scripts/crash_test.py --kills 100  # needs docker compose up first
```

- **Unit tests** cover the z-score and anomaly classification, and the storage consumer's write-then-commit logic using a fake Kafka consumer.
- **Integration tests** run the real upsert against a throwaway Postgres container (Testcontainers).
- **CI** runs lint, the tests and a 20-kill crash test on every push and pull request.

## Project structure

```
producer.py            Binance.US WebSocket → Kafka, reconnects with exponential backoff
anomaly_detector.py    crypto-trades → z-score anomaly detection → trade-analytics
storage_consumer.py    trade-analytics → Postgres (batched upserts, manual offset commits)
main.py                FastAPI: /trades, /anomalies, /anomalies/count, /dashboard
schema.sql             trades table, unique on (symbol, trade_id)
static/index.html      live dashboard
scripts/crash_test.py  delivery-guarantee crash test
tests/                 unit and integration tests
```

## Design decisions and limitations

- **Count-based window.** Binance.US trades too irregularly for a fixed time window to fill reliably, so the detector uses the last 30 trades, however long that takes. The window's time span varies as a result.
- **Cold start.** Nothing is flagged until the window has 30 trades. Rebuilding the window from Kafka history on restart would remove this blind spot.
- **The detector still auto-commits.** The delivery fix covers the storage consumer. A detector crash can still drop a few trades between the two topics; fixing it is next.
- **Prices are `DOUBLE PRECISION`.** Fine for anomaly scoring, but money should be stored as `NUMERIC`.
- **Malformed messages are logged and skipped** so one bad message can't block a partition. A dead-letter topic would keep them for inspection.
- **Polling dashboard.** The dashboard polls every 3 seconds. Server-Sent Events would push updates instead.
- **Kafka data is not on a named volume**, so `docker compose down -v` wipes the topics.

## Roadmap

- Apply the same delivery guarantee to the anomaly detector, and rebuild its window on restart
- Trade-ID gap checker to measure completeness against the exchange's own sequence numbers
- Robust anomaly model (log returns, median/MAD z-score)
- Replay harness for recorded trades, Server-Sent Events dashboard, Prometheus and Grafana
- 24/7 deployment
