# 1. Kafka between the pipeline stages

**Status:** accepted

## Context

Trades arrive continuously, and the producer, detector and storage consumer can each fail or restart on their own. A failure in one stage shouldn't lose data or stop the others. The detector also needs recent trades back after a restart to rebuild its windows.

## Decision

Put Kafka between every stage, with trades keyed by symbol. Writing straight from the producer to Postgres was the simpler alternative, but it ties ingestion to the database being up and leaves nothing to reprocess from.

## Consequences

- A stopped consumer picks up where it left off, and the detector rebuilds its windows from Kafka.
- Keys keep each symbol's trades in order, and leave room to run one detector per partition later.
- It adds a broker to run. This project runs one with no replication, so losing its disk loses what it holds.
