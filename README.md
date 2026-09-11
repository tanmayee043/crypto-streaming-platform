# Crypto Market Streaming & Anomaly Detection Platform

A real-time streaming pipeline that ingests live cryptocurrency trades, detects statistically unusual price and volume activity, and surfaces it on a live dashboard - built to demonstrate end-to-end data engineering and backend systems skills: stream processing, event-driven architecture, idempotent storage, and observability.

**[Demo video coming soon]**

---

## What it does

- Connects to a live public crypto exchange feed and streams trades through Kafka in real time
- Detects price and volume anomalies using a rolling statistical window (z-score analysis)
- Classifies anomalies by type (price / volume / both)
- Persists results to PostgreSQL with idempotent writes (safe against Kafka's at-least-once delivery)
- Serves processed data via a FastAPI backend
- Displays a live-updating dashboard with anomaly highlighting and real-time alerts

## Architecture

```
Binance.US public WebSocket (live trade feed)
        │
        ▼
Kafka topic: crypto-trades
        │
        ▼
Stream processor (sliding window, z-score anomaly detection)
        │
        ▼
Kafka topic: trade-analytics
        │
        ▼
Storage consumer  ──────────►  PostgreSQL
        │                          │
        ▼                          ▼
                    FastAPI  ◄──────┘
                        │
                        ▼
                Live dashboard (Chart.js)
```

Detection and storage are deliberately split into independent services connected only through Kafka, rather than one monolithic script - a failure or restart in one doesn't take down the others, and each piece could scale or be redeployed independently in a production setting.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python, `websockets`, Binance.US public API |
| Messaging | Apache Kafka (KRaft mode, no Zookeeper), `confluent-kafka` |
| Stream processing | Plain Python (sliding window + rolling z-score) |
| Storage | PostgreSQL, `psycopg2` |
| API | FastAPI |
| Dashboard | HTML/JS, Chart.js (served via FastAPI static files) |
| Infrastructure | Docker Compose |
| Observability | Python `logging`, Kafka UI |

## Metrics

Measured from a ~30-minute continuous run against Binance.US's live BTC/USDT feed:

- **Throughput:** ~1.15 trades/sec sustained (peaked at ~3 trades/sec during a burst of market activity). Binance.US's trade volume is noticeably variable - an earlier, quieter test session on the same pipeline saw as low as ~0.02 trades/sec, which is what motivated the count-based (rather than time-based) anomaly detection window described below.
- **Anomaly rate:** 91 anomalies out of 1,525 trades processed post-warmup ≈ **6.0%** (threshold: 3 standard deviations)
- **End-to-end latency (Kafka ingestion → Postgres write):** **p50 ≈ 32ms, p95 ≈ 40ms**, measured using a single clock (timestamped at the moment the producer receives each trade) to avoid clock-skew artifacts from comparing against Binance's own server timestamps
- **Reconnect resilience:** verified automatic recovery from simulated network drops via exponential backoff (1s–60s, resetting on a successful reconnect)


## Setup

**Prerequisites:** Python 3.10+, Docker Desktop, Git

```bash
git clone <repo-url>
cd crypto-streaming-platform
python -m venv venv
venv\Scripts\activate          # Windows; use source venv/bin/activate on Mac/Linux
pip install -r requirements.txt
```

Create a `.env` file (see `.env.example` for the required variables):
```
POSTGRES_USER=crypto
POSTGRES_PASSWORD=your_password_here
POSTGRES_DB=crypto_db
POSTGRES_PORT=5432
```

Start the infrastructure:
```bash
docker compose up -d
```

Create the Kafka topics:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic crypto-trades --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic trade-analytics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Load the database schema:
```bash
docker exec -i postgres psql -U crypto -d crypto_db < schema.sql
```

Run the pipeline (4 terminals):
```bash
python producer.py
python anomaly_detector.py
python storage_consumer.py
uvicorn main:app --reload
```

Visit:
- Dashboard: `http://localhost:8000/dashboard/`
- API docs: `http://localhost:8000/docs`
- Kafka UI: `http://localhost:8080`

## Limitations & design tradeoffs

These are the tradeoffs that come from building this within a defined scope:

- **Polling instead of WebSocket push to the dashboard.** The frontend refreshes every 3 seconds via HTTP polling rather than a persistent server-to-browser WebSocket. Simpler to build and reason about, at the cost of a few seconds of dashboard latency. A production version would likely push updates via a WebSocket or Server-Sent Events instead.
- **Window sizing tuned for Binance.US's liquidity.** Binance.US has meaningfully lower trade volume than the main Binance exchange, which made a fixed *time*-based window unreliable (it sometimes couldn't accumulate enough samples before data aged out). The anomaly detector uses a fixed *count*-based window instead (last 30 trades, however long that takes to arrive), trading a variable time horizon for robustness against inconsistent liquidity.
- **Cold-start blind spot.** Anomaly detection doesn't activate until the sliding window has enough samples to make a statistically meaningful comparison - a genuine anomaly occurring during this brief warm-up period would not be flagged. A future improvement would backfill the window from Kafka's existing history before switching to live processing.
- **Kafka topics are not persisted across a full volume reset** (`docker compose down -v`), since the Kafka service currently has no named volume - an intentional simplicity tradeoff for local development, not appropriate for production.

## What I'd improve with more time

- Push-based dashboard updates via WebSocket instead of polling
- Backfill the anomaly detector's window from Kafka history to eliminate the cold-start blind spot
- Migrate the stream processor from plain Python to Spark Structured Streaming for real horizontal scalability
- Add a proper CI pipeline (tests + linting on every push)
- Persist Kafka topics with a named volume for durability across restarts