# Deployment

**Not deployed yet.** The target is a free Oracle Cloud ARM VM. No VM exists so far because Oracle's free ARM capacity was full at the last attempt.

## Design

The production stack is in [`docker-compose.prod.yaml`](../docker-compose.prod.yaml):

- **One public entrance:** [Caddy](../deploy/Caddyfile) serves HTTPS with automatic certificates. Kafka and Postgres publish no ports; Prometheus, Grafana and Jaeger bind to the host's localhost, and `/metrics` is blocked at the entrance.
- **Limits for a public demo:** per-IP rate limits (health and metrics paths exempt) and caps on concurrent streams and replays. Nothing spends a paid quota, so no login is needed; visitors get the read-only dashboard and replays.
- **Secrets stay out of the repo:** [`setup-vm.sh`](../deploy/setup-vm.sh) generates random passwords on the VM, and deploy credentials live in GitHub's encrypted secrets.
- **Disk limits:** Kafka is configured to keep 7 days or 1 GB per partition, `retention.py` creates daily Postgres partitions and drops those older than 90 days, Docker logs rotate at 30 MB per service, and Prometheus keeps 35 days or 2 GB.
- **CI/CD:** after CI passes on `main`, [GitHub Actions](../.github/workflows/deploy.yml) builds the ARM image and pushes it to GitHub's registry. A deploy job, turned off until a VM exists, then deploys over SSH; [`deploy.sh`](../deploy/deploy.sh) applies migrations, waits for health checks, and rolls back if the new version is unhealthy.
- **Hosting the dashboard:** the API serves it rather than separate static hosting, so there's one address, no cross-origin setup, and the live streams just work.

Operating notes for the deployed stack: [`runbook.md`](runbook.md) (not yet exercised on a VM).

## What has and hasn't been tested

| Tested | Not tested until a VM exists |
|---|---|
| Stack on a laptop over plain HTTP: services healthy, security headers, Kafka and Postgres closed, `/metrics` returns 404 | Real HTTPS certificates |
| SSE and replay through Caddy; rate limit returned 429s under 400 parallel requests; 6/6 Prometheus targets up | `setup-vm.sh`, reboot recovery, external uptime monitoring |
| ARM image builds in CI; imports checked under emulation | Deploy job, `deploy.sh` migrations and rollback; running on real ARM hardware |
| Dropping old partitions (integration tests) | Disk limits under sustained load; load test on the VM |
