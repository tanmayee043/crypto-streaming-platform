# Crypto Market Streaming & Anomaly Detection Platform

[![CI](https://github.com/tanmayee043/crypto-streaming-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/tanmayee043/crypto-streaming-platform/actions/workflows/ci.yml)

A real-time pipeline that streams live crypto trades from Binance.US through Kafka, flags unusual price moves and unusually large trades per symbol, stores every trade in PostgreSQL exactly once, and measures its own completeness against the exchange's trade IDs.

**[Demo video coming soon]**

## The problem

An exchange's surveillance team needs to see abnormal trading *as it happens*, and to prove to compliance that the system never silently dropped a trade. Spotting spikes is the easy half. This project is built around the hard half: a guarantee that every trade is stored exactly once, even through crashes, and a measurement of exactly which trades were missed.

## Highlights

- **No lost or duplicated trades, proven end to end:** the storage consumer and the anomaly detector were each hard-killed 100 times while trades were flowing. 0 missing, 0 duplicates, and every anomaly score matched an uninterrupted run.
- **Fewer false alarms, more real anomalies caught:** on 60,000 real trades the new model flags 2.9% of trades instead of 4.6%, and catches 95% of planted price spikes instead of 72%. The gains hold on a separate 60,000-trade hold-out set.
- **Completeness you can measure:** Binance trade IDs are sequential per symbol, so a gap checker reports exactly which trades are missing and the % captured.
- **Found and fixed silent data loss:** the original storage consumer lost 67% of trades during a single Postgres restart. Now 0.
- **Fast enough to spare:** sustains about 5,850 trades/s end to end on a laptop (live Binance.US traffic is under 1 trade/s), with p95 latency under 2 seconds at 1,000 trades/s.
- **Replayable:** recorded real trades can be replayed at 10×/100×/1000×, from the dashboard or the command line. Replaying the 10 Oct 2025 crash (10,507 real trades) at 1000× takes 22 seconds and flags 7.0% of trades, against 1.5–2.9% on ordinary days.
- **Live and observable:** the dashboard is pushed new trades and anomalies over Server-Sent Events, shows pipeline health against its SLOs, and explains every flag. Prometheus, a provisioned Grafana dashboard and OpenTelemetry traces cover the operator's side.

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
                   PostgreSQL ──► FastAPI (main.py) ──SSE──► dashboard (static/index.html)
                        ▲                 │  └─► alerts: Discord webhook + browser notifications
                 gap_checker.py           └─► OpenTelemetry traces ──► Jaeger
        missing trade IDs, completeness %, consumer lag

   every service ──/metrics──► Prometheus ──► Grafana dashboard (SLOs, per-stage latency, lag)
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
| [Storage consumer](scripts/crash_test.py) (synthetic, 2,000/s) | 100 (86 mid-write) | 162,969 | 162,969 | **0** | **0** | n/a |
| [Anomaly detector](scripts/detector_crash_test.py) (real trades, 500/s) | 100 (62 mid-batch) | 60,000 | 60,000 | **0** | **0** | **0** |

**The bug this fixed:** both consumers originally relied on Kafka's auto-commit, which commits offsets on a timer whether or not the work succeeded. In a test with one Postgres restart, the original storage consumer lost 2,066 of 3,070 trades; the fixed one lost none. The original detector also never published the first 29 trades after each start, so they never reached the database.

### Before and after

| | Original | Now |
|---|---|---|
| Offsets committed | On a 5-second timer, whether or not the write worked (auto-commit) | Only after the write is durable |
| One Postgres restart while trades flow | **2,066 of 3,070 trades lost (67%)**, silently | **0 lost**: reconnects and retries the same batch |
| 100 hard kills of each consumer | Not tested | **0 missing, 0 duplicates, 0 wrong scores** |
| First trades after each detector start | First 29 never stored; blind spot while warming up | All stored; window rebuilt from Kafka |
| Flag rate on 60,000 real trades | 4.63% | **2.89%** |
| Planted price spikes caught | 72.2% | **95.2%** |
| Knowing what was missed | Couldn't tell | Exact missing trade IDs and % captured |
| Maximum sustained throughput | Not measured | **about 5,850 trades/s** |

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

[`scripts/load_test.py`](scripts/load_test.py) replays 60,000 real trades through the real detector and storage consumer and measures latency from receipt to Postgres, per trade, on one clock (so no clock skew). Measured on a Windows laptop with Kafka and Postgres in Docker Desktop:

| Load | Throughput | Latency p50 | Latency p95 | Latency p99 |
|---|---|---|---|---|
| Live feed, 4 symbols (~0.3 trades/s, 5-min run) | live | | 48 ms | |
| Steady 1,000 trades/s (several runs) | ~1,000 trades/s | 14–199 ms | 27–254 ms | 75–431 ms |
| As fast as possible (2 runs) | **~5,850 trades/s** | backlog | backlog | backlog |

All of these are well inside the 2-second p95 target. Latency at 1,000 trades/s is bimodal: a run settles at either about 15 ms or about 200 ms and stays there. The per-stage metrics show the slower mode in both consumers at once, and the Postgres write itself stays around 5–10 ms, so the database isn't the cause; the root cause is still open, and numbers from the Linux VM will show whether it's specific to this setup.

**A profile found the biggest speed-up.** Python's `statistics.mean` does exact-fraction arithmetic, and it was the slowest part of the detector. Replacing it with `math.fsum` took the detector's own code from about 3,000–7,000 to about 24,000 trades/s and the whole pipeline from about 3,400 to about 5,850 trades/s, with identical model results on both evaluation sets.

## Dashboard

A single page (`static/index.html`, plain HTML/CSS/JS with Chart.js, served by FastAPI) built for a market-surveillance analyst.

- **Live push:** `GET /stream` sends new trades, anomalies and health as Server-Sent Events. One background loop in the API reads new rows twice a second and fans them out to every open browser; a browser that reconnects catches up on anything it missed from the last row ID it saw.
- **Price chart with anomaly markers** that differ by shape as well as colour (price ▲, size ◆, both ★).
- **Health strip:** trades/s, p95 latency, completeness and consumer lag, each against its target.
- **Anomaly drawer:** the trade, its scores, the trades around it as a mini chart, and plain-English reasons it was or wasn't flagged.
- **Anomaly history** with symbol, type and date filters and cursor pagination; a **pipeline health** view with per-symbol completeness and consumer lag.
- **Replay mode:** pick a recording and 10×/100×/1000×. It runs the same model code in the API process and streams the results; nothing is written to Kafka or Postgres, and the page labels it as a replay. When the live feed is offline, the page offers a replay of the 10 Oct 2025 crash instead of an empty screen.
- Light and dark themes, keyboard navigation, reduced-motion support, and a layout that works at 375 px. Checked in a browser at desktop and phone widths, with no console errors; during a 1000× replay the page has no long tasks, because updates are drawn at most four times a second.

## Alerts

`alerts.py` posts to a Discord webhook (`DISCORD_WEBHOOK_URL`) when anomalies arrive (batched to at most one message a minute) and once when an SLO is breached, then once more when it recovers. The dashboard can also raise browser notifications for new anomalies while the tab is in the background.

## Observability

```bash
docker compose --profile observability up -d     # Prometheus :9090, Grafana :3000, Jaeger :16686
```

- **Prometheus metrics** from every service (`producer` :8001, `detector` :8002, `storage` :8003, `gap_checker` :8004, API `/metrics`): throughput, anomalies, dead letters, write retries, reconnects, consumer lag, completeness, freshness, and latency histograms per stage (producer receipt → detector result in Kafka → row stored). The metrics only listen on localhost.
- **Grafana** loads the "Crypto streaming pipeline" dashboard automatically ([`observability/`](observability/)): SLO stats, throughput, latency p50/p95 and where the time goes per stage, consumer lag, completeness, anomalies and errors.
- **OpenTelemetry** traces every API request and its SQL queries and sends them to Jaeger when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. The live-stream polling loop is excluded so it doesn't bury real requests.
- Liveness and readiness at `/healthz` and `/readyz`.

## Deployment

Designed to run 24/7 on a free Oracle Cloud ARM VM. Operating it: [`docs/runbook.md`](docs/runbook.md).

- **Only HTTPS is public.** [Caddy](deploy/Caddyfile) is the single entrance and handles certificates automatically. Kafka and Postgres publish no ports; Prometheus, Grafana and Jaeger are bound to the VM's localhost and reached through an SSH tunnel. `/metrics` is blocked at the entrance.
- **Limits for a public demo:** per-IP rate limits on every endpoint and caps on concurrent streams and replays. There's nothing that spends a paid quota, so no login is needed; visitors get the read-only dashboard and replays.
- **Secrets stay out of the repo:** random passwords are generated on the VM, and deploy credentials live in GitHub's encrypted secrets.
- **The disk can't fill:** Kafka keeps 7 days or 1 GB per partition, `retention.py` creates daily Postgres partitions and drops those older than 90 days (instant, unlike row deletes), Docker logs rotate at 30 MB per service, and Prometheus keeps 35 days or 2 GB.
- **CI/CD:** after CI passes on `main`, [GitHub Actions](.github/workflows/deploy.yml) builds the ARM image, pushes it to GitHub's registry and deploys over SSH; [`deploy.sh`](deploy/deploy.sh) applies migrations, waits for health checks, and rolls back if the new version is unhealthy.
- **Restarts:** Docker starts on boot and every service restarts automatically. A restart at any moment is safe, which is what the crash tests prove.
- **External monitoring:** UptimeRobot checks `/readyz` from outside, because a system can't report its own downtime.

The dashboard is served by the API itself rather than from separate static hosting: same address, no cross-origin setup, and the live streams just work. A shared React app with static hosting is a possible later step.

**On the completeness claim:** the target is 99.9% of trades captured over 30 days. That figure can only be quoted after the system has really run that long, verified by trade-ID checks and external uptime monitoring. Until then, this README reports only what has been measured.

## What breaks at 10×

Live Binance.US traffic is well under 1 trade/s, so 10× live is nothing; the interesting question is 10× the load test, about 58,000 trades/s:

- **The detector goes first:** it's single-threaded Python. Kafka's keys already let it scale out per partition, so the next steps are more partitions, one detector per partition, and an incremental median instead of re-sorting 100 values per trade.
- **Hash partitioning gets lumpy:** BTC and ETH share a partition today. Busy symbols would need explicit placement.
- **Storage is next:** one consumer doing batched upserts. Several would work per partition, but the live stream currently relies on row IDs committing in order, so it would need Postgres `LISTEN/NOTIFY` or a Kafka-backed feed.
- **A single VM is the real ceiling:** one broker with no replication means a disk failure loses what's in Kafka. Real scale means three brokers, replication and managed Postgres, which is also where "free" ends.
- **Browsers:** every event goes to every open dashboard. Past a few thousand trades/s the server should send aggregates (say, one point per second), not every trade.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python, `websockets`, Binance.US public API |
| Messaging | Apache Kafka 4.3 (KRaft), `confluent-kafka`, dead-letter topics |
| Stream processing | Plain Python (per-symbol windows, robust z-score) |
| Storage | PostgreSQL 16 (`NUMERIC` prices), `psycopg2` |
| API and dashboard | FastAPI (REST + Server-Sent Events), HTML/CSS/JS with Chart.js |
| Observability | Prometheus, Grafana, OpenTelemetry + Jaeger, Discord webhook alerts |
| Testing and CI | pytest, Testcontainers, ruff, GitHub Actions |
| Infrastructure | Docker Compose (pinned image versions), Caddy, Oracle Cloud Always Free ARM VM, GitHub Actions + GitHub's registry |

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
python retention.py
uvicorn main:app --reload
```

Then open the dashboard at `http://localhost:8000/dashboard/`, the API docs at `http://localhost:8000/docs`, or Kafka UI at `http://localhost:8080`. For Grafana and tracing, see [Observability](#observability).

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
| `DISCORD_WEBHOOK_URL` | (empty) | Where alerts go; empty means log only |
| `SLO_COMPLETENESS_PCT` / `SLO_LATENCY_P95_SECONDS` / `SLO_CONSUMER_LAG` | `99.9` / `2` / `1000` | Targets for the health strip, alerts and Grafana |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset) | Turns on API tracing, e.g. `http://localhost:4318` |

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
- **Integration tests** run against a throwaway Postgres container (Testcontainers): exact `NUMERIC` prices, idempotent upserts, all-or-nothing batches, reconnects, the gap checker, and the API (history filters and paging, the detail drawer's data, health, and the live loop pushing new rows to connected clients).
- **API and alert tests:** flag explanations, the event format, replay streaming, replay files limited to their folders, Discord batching, breach-once-and-recover, and a real HTTP post to a fake webhook.
- **CI** runs lint, the tests, a 20-kill storage crash test and a 10-kill detector crash test on every push and pull request.

## Project structure

```
producer.py              Binance.US WebSocket → Kafka, keyed by symbol, reconnects with backoff
anomaly_detector.py      crypto-trades → per-symbol anomaly model → trade-analytics
storage_consumer.py      trade-analytics → Postgres (batched upserts, manual offset commits)
gap_checker.py           missing trade IDs, completeness %, consumer lag and freshness (+ metrics)
retention.py             creates daily Postgres partitions, drops old ones
rate_limit.py            per-IP rate limiting for the public API
alerts.py                Discord alerts for anomalies and SLO breaches
kafka_helpers.py         offset, batching and dead-letter helpers shared by both consumers
main.py                  FastAPI: REST, /stream (SSE), /health/pipeline, /replay, /metrics, /dashboard
schema.sql, migrations/  database schema and upgrades for existing databases
static/index.html        dashboard
observability/           Prometheus config, Grafana provisioning and dashboard
Dockerfile, docker-compose.prod.yaml, deploy/   production image, stack, Caddy, VM setup and deploy scripts
docs/runbook.md          how to operate the deployed system
replays/                 recorded real trades for replay mode (10 Oct 2025 crash)
scripts/                 recording, replay, model evaluation, crash tests, load test
tests/                   unit and integration tests, sample trade file
```

## Design decisions and limitations

- **Hash partitioning with few symbols is uneven.** With `btcusdt,ethusdt,solusdt,xrpusdt`, BTC and ETH share partition 0, so one detector instance per partition wouldn't be perfectly balanced.
- **Window rebuild reads the last 2,000 messages per partition.** That is exact while each symbol had at least 101 trades in those messages; a very rare symbol sharing a partition with busy ones could restart with a partly filled window.
- **Masking in degenerate windows.** When more than half of a window is identical, one huge outlier can still raise the scale enough to hide the next anomaly.
- **Thresholds were tuned on historical data** (checked on a hold-out set). They should be re-checked periodically as market conditions change.
- **Dead letters are kept but not yet shown** on the dashboard; they can be read with Kafka's tools.
- **Gaps are reported, not yet repaired.** Backfilling missing trades from the exchange's REST API is planned for the Lakehouse project.
- **One broker, no replication.** Production keeps Kafka on a named volume, but a single VM is still a single point of failure; Prometheus data and backups live on the same disk.
- **In development, Kafka has no named volume**, so recreating its container wipes the topics.
- **The live stream follows row IDs.** That's exact with one storage consumer (its transactions commit in ID order); several storage consumers in parallel could commit IDs out of order, and Postgres `LISTEN/NOTIFY` would be the next step.
- **Alerts run inside the API process**, so they stop if the API stops. In production, SLO alerts would come from Prometheus/Grafana alerting, which keeps working when the API is down.
- **Large orders flag several times.** One big order that fills against many resting orders creates many trades with the same timestamp, and each fill can be flagged as a size anomaly (visible in the crash replay). Grouping fills by timestamp and side before scoring would report it once.
- **Replay mode in the UI scores in the API process**, not through Kafka; full-pipeline replays use `scripts/replay.py`.

## Roadmap

- Run 30 days and publish the real completeness figure
- Explain why latency at 1,000 trades/s settles at either about 15 ms or 200 ms between runs
- Group fills of one order before scoring
- Automatic backfill of gaps from the exchange's REST API
