# 7. Oracle Cloud Always Free for hosting

**Status:** accepted; not deployed yet

## Context

The pipeline has to run 24/7 for a real 30-day completeness figure, on free tiers only. Kafka and Postgres together need a reasonable amount of memory.

## Decision

Target Oracle Cloud's Always Free ARM VM (up to 2 OCPUs and 12 GB of memory in this project's setup), running the Docker Compose production stack behind Caddy.

## Consequences

- It costs nothing, as long as it stays inside the free limits.
- Free ARM capacity is often full. No VM has been obtained yet, so a retry job keeps asking for one.
- Images have to be built for ARM.
- One VM is a single point of failure, which is acceptable for a portfolio demo but not for real surveillance.
