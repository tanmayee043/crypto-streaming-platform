# Runbook

How to operate the deployed stack. All commands run on the VM unless noted:

```bash
ssh -i ~/.ssh/oracle_vm ubuntu@YOUR_VM_IP      # from your laptop
cd /opt/crypto-streaming
alias dc='docker compose -f docker-compose.prod.yaml'
```

## See what's going on

| Question | Command |
|---|---|
| Is everything up? | `dc ps` |
| A service's recent logs | `dc logs --tail 50 storage` (or `producer`, `detector`, `gap-checker`, `retention`, `api`, `caddy`, `kafka`, `postgres`) |
| Consumer lag | `dc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --all-groups` |
| Completeness now | `curl -s https://YOUR_ADDRESS/completeness` |
| Disk use | `df -h /` and `docker system df` |

**Grafana, Prometheus and Jaeger are not public.** Reach them through an SSH tunnel from your laptop:
```bash
ssh -i ~/.ssh/oracle_vm -L 3000:localhost:3000 -L 9090:localhost:9090 -L 16686:localhost:16686 ubuntu@YOUR_VM_IP
```
Then open <http://localhost:3000> (Grafana; user `admin`, password from `grep GRAFANA /opt/crypto-streaming/.env`), <http://localhost:9090> or <http://localhost:16686>.

## Deploy

Merging to `main` deploys automatically once CI passes (Actions → Deploy). To redeploy by hand: Actions → Deploy → Run workflow.

## Roll back

`deploy.sh` rolls back by itself if a new version isn't healthy. To go back by choice, find an earlier commit's image (GitHub → Packages; the newest 3 are kept) and run:
```bash
docker login ghcr.io -u YOUR_GITHUB_USERNAME     # only needed for a private repo; paste a token with read:packages
bash deploy/deploy.sh ghcr.io/tanmayee043/crypto-streaming-platform:COMMIT_SHA
docker logout ghcr.io
```

## The three most likely failures

### 1. The dashboard says "live feed offline" / completeness is dropping
The producer lost Binance, or something between it and Postgres stopped.
1. `dc ps`: is anything not running? `dc up -d --no-build` starts it.
2. `dc logs --tail 50 producer`: "Connection lost" lines mean it's reconnecting with backoff, which is normal for short periods.
3. Consumer lag growing? `dc logs --tail 50 detector` and `storage`. A restart is safe at any moment (that's what the crash tests prove): `dc restart detector storage`.

Trades missed while the producer was down are recorded in `trade_gaps`. They aren't lost for good: the exchange's history API has them, and automatic backfill comes with the Lakehouse project.

### 2. The disk is filling up
Limits are in place (Kafka: 7 days or 1 GB per partition; Postgres: `RETENTION_DAYS`; logs: 30 MB per service; Prometheus: 35 days or 2 GB), so check they're working:
1. `df -h /` and `docker system df -v` to see what's big.
2. `dc logs --tail 5 retention` should show an hourly line. If not, `dc restart retention`.
3. Need room now? Lower `RETENTION_DAYS` in `.env` (keep it above 30 while you're building the 30-day record), then `dc up -d --no-build retention`.
4. `docker image prune -af` removes unused old images.

### 3. The site is unreachable or the HTTPS certificate fails
1. Is it the whole VM? Try SSH. If that fails too, check the instance in the Oracle console (and start it if Oracle stopped it).
2. `dc logs --tail 50 caddy`: certificate errors usually mean DuckDNS points at the wrong IP (update it at duckdns.org) or ports 80/443 are closed in Oracle's security list.
3. `curl -s localhost:80 -H "Host: YOUR_ADDRESS" -I` on the VM should answer; if it does, the problem is outside the VM.

## Back up the database

```bash
dc exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' | gzip > backup-$(date +%F).sql.gz
```
Then copy it to your laptop (from your laptop): `scp -i ~/.ssh/oracle_vm ubuntu@YOUR_VM_IP:/opt/crypto-streaming/backup-*.sql.gz .`

## Load test on the VM

Runs in a throwaway `load_test` database and topics, so live data isn't touched, but it does use the VM's CPU for a minute or two:
```bash
dc run --rm api python scripts/load_test.py --file replays/crash-2025-10-10.jsonl --rate 1000
dc run --rm api python scripts/load_test.py --file replays/crash-2025-10-10.jsonl --rate 0
```
