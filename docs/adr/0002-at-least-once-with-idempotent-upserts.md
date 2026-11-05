# 2. At-least-once delivery with idempotent upserts

**Status:** accepted

## Context

The original consumers used Kafka's auto-commit, which commits offsets on a timer whether or not the work succeeded. In a one-off test, one Postgres restart lost 2,066 of 3,070 trades.

## Decision

Commit offsets manually, only after the work is durable, and make the Postgres write an upsert on `(symbol, trade_id, trade_time)`. A crash then reprocesses a batch, and the upsert absorbs the repeats.

Kafka transactions were the main alternative, but they only cover writes to Kafka, and the final write here goes to Postgres.

## Consequences

- Crash tests hard-killed each consumer 100 times with 0 missing and 0 duplicate rows.
- Every write path has to stay idempotent.
- Trades the producer never receives aren't covered, so the gap checker reports them separately.
