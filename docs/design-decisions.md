# Design decisions and limitations

## Stream processing

- **Hash partitioning with few symbols is uneven.** With `btcusdt,ethusdt,solusdt,xrpusdt`, BTC and ETH share partition 0, so one detector instance per partition wouldn't be perfectly balanced. Busy symbols would eventually need explicit placement.
- **Window rebuild reads the last 2,000 messages per partition.** That is exact while each symbol had at least 101 trades in those messages; a very rare symbol sharing a partition with busy ones could restart with a partly filled window.
- **Dead letters are kept but not yet shown** on the dashboard; they can be read with Kafka's tools.

## Anomaly model

- **Log returns and a robust scale.** Log returns make a move mean the same at any price level. The median, with a scale floor that resists lumpy trade data, makes it harder for one big move to hide the next.
- **Masking in degenerate windows.** When more than half of a window is identical, one huge outlier can still raise the scale enough to hide the next anomaly.
- **Thresholds were tuned on historical data** (checked on a hold-out set). They should be re-checked periodically as market conditions change.
- **Large orders flag several times.** One big order that fills against many resting orders creates many trades with the same timestamp, and each fill can be flagged as a size anomaly (visible in the crash replay). Grouping fills by timestamp and side before scoring would report it once.
- **A profile found the biggest speed-up.** Python's `statistics.mean` does exact-fraction arithmetic and was the slowest part of the detector. Replacing it with `math.fsum` took the best full-pipeline run from about 3,550 to about 5,850 trades/s, with identical model results on both evaluation sets.

## Storage and completeness

- **Gaps are reported, not yet repaired.** Backfilling missing trades from the exchange's REST API is planned.
- **The live stream follows row IDs.** That's exact with one storage consumer (its transactions commit in ID order); several storage consumers in parallel could commit IDs out of order, and Postgres `LISTEN/NOTIFY` would be the next step.

## Infrastructure

- **One broker, no replication.** The production stack keeps Kafka on a named volume, but a single VM is still a single point of failure; Prometheus data, and backups unless copied off, live on the same disk.
- **In development, Kafka has no named volume**, so recreating its container wipes the topics.
- **Alerts run inside the API process**, so they stop if the API stops. Alerting from Prometheus/Grafana would keep working when the API is down.
- **Replay mode in the UI scores in the API process**, not through Kafka; full-pipeline replays use `scripts/replay.py`.
- **Every event goes to every open dashboard.** Past a few thousand trades/s the server should send aggregates (say, one point per second), not every trade.
- **Discord alerts** are tested against a local fake webhook, not yet a real Discord channel.

## What breaks at 10×

This is design reasoning, not a test result. 10× live traffic is nothing, so the interesting question is 10× the load test's burst rate, about 58,000 trades/s:

- **The detector probably goes first:** it's single-threaded Python. Next steps are more partitions, one detector per partition, and an incremental median.
- **Storage is next:** several consumers could work per partition, but the live stream relies on row IDs committing in order, so it would need Postgres `LISTEN/NOTIFY` or a Kafka-backed feed.
- **A single machine is the real ceiling:** real scale means three brokers, replication and managed Postgres, which is also where "free" ends.

## Next steps

- Deploy to the VM and complete the [untested deployment items](deployment.md#what-has-and-hasnt-been-tested)
- Record a demo video
- Run 30 days and publish the real completeness figure
- Explain why p50 at 1,000 trades/s lands at either about 15 ms or about 200 ms, and confirm the cold-start delay
- Group fills of one order before scoring
- Automatic backfill of gaps from the exchange's REST API
