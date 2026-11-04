#!/usr/bin/env bash
# One-time setup of a fresh Oracle Ubuntu VM. Safe to run again.
#   bash setup-vm.sh yourname.duckdns.org
set -euo pipefail

SITE_ADDRESS="${1:?Usage: bash setup-vm.sh yourname.duckdns.org}"
APP_DIR=/opt/crypto-streaming

echo "==> Installing Docker (official install script)"
if ! command -v docker > /dev/null; then
    curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER"
sudo systemctl enable --now docker   # Docker, and so the whole stack, comes back after a reboot

echo "==> Opening ports 80 and 443 in the VM's own firewall (Oracle's images block them by default)"
for port in 443 80; do
    if ! sudo iptables -C INPUT -p tcp --dport "$port" -m state --state NEW -j ACCEPT 2> /dev/null; then
        sudo iptables -I INPUT 5 -p tcp --dport "$port" -m state --state NEW -j ACCEPT
    fi
done
sudo apt-get install -y -qq iptables-persistent > /dev/null
sudo netfilter-persistent save > /dev/null

echo "==> Automatic security updates"
sudo apt-get install -y -qq unattended-upgrades > /dev/null
sudo systemctl enable --now unattended-upgrades

echo "==> Creating $APP_DIR and its secrets"
sudo mkdir -p "$APP_DIR"
sudo chown "$USER" "$APP_DIR"
if [ ! -f "$APP_DIR/.env" ]; then
    # random passwords, created here and never copied anywhere else
    cat > "$APP_DIR/.env" <<EOF
SITE_ADDRESS=$SITE_ADDRESS
POSTGRES_USER=crypto
POSTGRES_PASSWORD=$(openssl rand -hex 24)
POSTGRES_DB=crypto_db
POSTGRES_PORT=5432
GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 16)
SYMBOLS=btcusdt,ethusdt,solusdt,xrpusdt
RETENTION_DAYS=90
DISCORD_WEBHOOK_URL=
EOF
    chmod 600 "$APP_DIR/.env"
    echo "    Created $APP_DIR/.env"
else
    echo "    $APP_DIR/.env already exists, leaving it alone"
fi

echo
echo "Done. Log out and back in once (so the docker group applies), then deploy."
