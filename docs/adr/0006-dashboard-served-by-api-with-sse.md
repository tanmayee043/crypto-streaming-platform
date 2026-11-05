# 6. Dashboard served by the API, with Server-Sent Events

**Status:** accepted

## Context

The dashboard needs live trades, anomalies and health. Data only flows from server to browser, and hosting should stay simple and free.

## Decision

Serve a single HTML page from FastAPI, and push updates over Server-Sent Events. WebSockets would add a two-way channel nothing needs, and separate static hosting would add cross-origin setup.

## Consequences

- There's one address, no build step, and browsers reconnect by themselves, catching up from the last row ID they saw.
- The live stream follows row IDs, which is exact only with one storage consumer. Several would need Postgres `LISTEN/NOTIFY` or a Kafka-backed feed.
- Every event goes to every open dashboard, so much higher traffic would need aggregates.
- A shared React front end across projects would be a later step.
