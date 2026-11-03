# Deploying to Oracle Cloud (free)

This puts the whole pipeline on a free Oracle VM, running 24/7, at a free HTTPS address. Everything here is free. **You do the account steps yourself**: nobody else should ever enter your card details or passwords.

**What stays free, and how to keep it that way**

| Piece | Free because |
|---|---|
| Oracle VM | "Always Free" ARM shape. Oracle asks for a card to verify you, but Always Free resources aren't charged. **Don't click "Upgrade to Pay As You Go".** Only create resources labelled "Always Free-eligible". |
| Web address | DuckDNS subdomain |
| HTTPS certificate | Let's Encrypt, handled by Caddy |
| Builds and image storage | GitHub Actions and GitHub's registry. Unlimited on a public repo. On a private repo there are monthly limits; the workflow deletes old images to stay under them, and GitHub can't charge you without a payment method on file. |
| Uptime monitoring | UptimeRobot free plan |

Oracle's trial also gives you credits for the first 30 days. Ignore them; nothing here needs them, and Always Free resources keep running after the trial ends.

**Know this about Oracle's free tier:** Oracle's policy allows it to reclaim Always Free VMs that sit idle (very low CPU, network and memory over 7 days). This stack does steady work and holds Kafka and Postgres in memory, so it shouldn't count as idle, but check [Oracle's Always Free page](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm) for the current rules. Your data lives on the VM, so take the occasional backup (see the [runbook](runbook.md)).

---

## 1. Create the Oracle account (you, about 15 minutes)

1. Go to <https://www.oracle.com/cloud/free/> and click **Start for free**.
2. Enter your details and verify your email.
3. **Home region:** think before choosing, because it can't be changed and free ARM capacity varies by region. Pick one near you. If you've seen reports of a region with ARM capacity, prefer it.
4. Enter your card when asked. It's for identity verification; a small temporary hold may appear.
5. Wait for "Your account is ready" (minutes to a few hours).

## 2. Make an SSH key on your laptop (you)

In PowerShell:
```powershell
ssh-keygen -t ed25519 -f $HOME\.ssh\oracle_vm -C "oracle-vm"
```
Press Enter for no passphrase, or set one. This creates `oracle_vm` (private: never share it) and `oracle_vm.pub` (public: safe to paste).

## 3. Create the free VM (you)

In the Oracle console: **Compute → Instances → Create instance**.

1. **Image and shape → Edit.**
   - Image: **Canonical Ubuntu 24.04** (the aarch64 build).
   - Shape: **Ampere → VM.Standard.A1.Flex**. It must show **Always Free-eligible**.
   - Size: **2 OCPUs, 12 GB memory**. Oracle's free limits have changed over time; if the console shows a lower free limit, stay within it.
2. **Networking:** keep the default new VCN, with **Assign a public IPv4 address** on.
3. **Add SSH keys → Paste public keys:** paste the contents of `oracle_vm.pub`:
   ```powershell
   Get-Content $HOME\.ssh\oracle_vm.pub
   ```
4. **Boot volume:** the default is fine (free up to 200 GB total).
5. Click **Create**, then copy the **public IP address** when it's running.

**"Out of capacity" error?** This is common for free ARM VMs. Try another availability domain in the same screen, or retry at a different time of day. If it never works, use the plan's fallback: run the stack on your laptop for the demo video (`docker compose -f docker-compose.prod.yaml` works there too) and keep retrying.

## 4. Open ports 80 and 443 in Oracle's network (you)

Instance page → click the **subnet** → **Security Lists** → the default list → **Add Ingress Rules**, twice:

| Source CIDR | IP protocol | Destination port |
|---|---|---|
| `0.0.0.0/0` | TCP | `80` |
| `0.0.0.0/0` | TCP | `443` |

Don't add any others. Port 22 (SSH) is already open; Kafka, Postgres, Grafana and the rest must stay closed.

## 5. Get a free web address (you)

1. Go to <https://www.duckdns.org> and sign in.
2. Add a subdomain, e.g. `yourname`, giving `yourname.duckdns.org`.
3. Paste the VM's public IP in **current ip** and click **update ip**.

## 6. Set up the VM (you run it; the script does the work)

From the repo folder on your laptop:
```powershell
scp -i $HOME\.ssh\oracle_vm deploy\setup-vm.sh ubuntu@YOUR_VM_IP:
ssh -i $HOME\.ssh\oracle_vm ubuntu@YOUR_VM_IP
```
On the VM:
```bash
bash setup-vm.sh yourname.duckdns.org
exit
```
It installs Docker (and enables it on boot), opens 80/443 in the VM's own firewall, turns on automatic security updates, and creates `/opt/crypto-streaming/.env` with random passwords that exist only on the VM.

## 7. Give GitHub permission to deploy (you)

Make a separate key just for deployments, and allow it on the VM:
```powershell
ssh-keygen -t ed25519 -f $HOME\.ssh\crypto_deploy -N '""' -C "github-actions-deploy"
Get-Content $HOME\.ssh\crypto_deploy.pub | ssh -i $HOME\.ssh\oracle_vm ubuntu@YOUR_VM_IP "cat >> ~/.ssh/authorized_keys"
ssh-keyscan YOUR_VM_IP
```
In GitHub: repo → **Settings → Secrets and variables → Actions**.

**Secrets** (New repository secret):

| Name | Value |
|---|---|
| `DEPLOY_HOST` | the VM's public IP |
| `DEPLOY_USER` | `ubuntu` |
| `DEPLOY_SSH_KEY` | all of `$HOME\.ssh\crypto_deploy` (the private one) |
| `DEPLOY_KNOWN_HOSTS` | the full output of the `ssh-keyscan` command above |

Secrets stay encrypted in GitHub and never appear in logs.

**Recommended (free and simple):** make the repo public: Settings → General → Danger Zone → Change visibility. It has no secrets in it, and public repos get unlimited Actions minutes and image storage. A portfolio project wants to be public anyway.

## 8. Go live (you decide when)

**This is the moment it becomes public.** In GitHub: **Settings → Secrets and variables → Actions → Variables → New repository variable**: `DEPLOY_ENABLED` = `true`.

Then **Actions → Deploy → Run workflow** (main branch). After this, every merge to `main` that passes CI deploys by itself, and rolls back automatically if the new version isn't healthy.

## 9. Check it (you, about 5 minutes)

1. Open `https://yourname.duckdns.org`. You should see the padlock and the dashboard.
2. `https://yourname.duckdns.org/metrics` must say **not found**.
3. From your laptop, these must fail (connection refused or timeout):
   ```powershell
   Test-NetConnection YOUR_VM_IP -Port 5432
   Test-NetConnection YOUR_VM_IP -Port 29092
   Test-NetConnection YOUR_VM_IP -Port 3000
   ```
4. **Reboot test:** `ssh -i $HOME\.ssh\oracle_vm ubuntu@YOUR_VM_IP sudo reboot`, wait 3 minutes, and refresh the dashboard. It should recover by itself.

## 10. External uptime monitoring (you, free)

A system can't report its own downtime, so something outside must watch it.

1. Create a free account at <https://uptimerobot.com>.
2. **New monitor** → type **HTTP(s)** → URL `https://yourname.duckdns.org/readyz` → interval **5 minutes**.
3. Add your email as the alert contact.

`/readyz` also checks the database, so it fails if the API can't reach Postgres.

## 11. Discord alerts (you, optional, free)

In Discord: channel settings → **Integrations → Webhooks → New Webhook → Copy URL**. Then on the VM:
```bash
cd /opt/crypto-streaming
nano .env        # paste the URL after DISCORD_WEBHOOK_URL=
docker compose -f docker-compose.prod.yaml up -d --no-build api
```
The URL is a password in disguise: keep it only in that file.

## 12. After it's running

- Let it run. The "99.9% over 30 days" completeness figure can only be claimed **after it has really run 30 days**; until then, quote the number and how many days it covers.
- Run the load test on the VM for deployment numbers (see the [runbook](runbook.md)), then update the README.
- Record the demo: [demo-script.md](demo-script.md).
