# 5. Postgres partitioned by day

**Status:** accepted

## Context

The system is meant to run continuously on a small VM, so old trades have to go without slowing writes. Prices also have to stay exact.

## Decision

Partition `trades` by day on `trade_time`, and let `retention.py` create upcoming partitions and drop those older than 90 days. Store prices as `NUMERIC(20,8)`, never floats.

## Consequences

- Dropping a partition is much cheaper than deleting rows, and it frees the disk space.
- Postgres requires the partition key in unique constraints, so the upsert key became `(symbol, trade_id, trade_time)`. This relies on a trade's exchange timestamp never changing.
- `RETURNING (xmax = 0)` doesn't work on partitioned tables, so new rows are detected with `created_at = now()`.
- There's one more service to run, and trades for a day with no partition go to the default partition.
