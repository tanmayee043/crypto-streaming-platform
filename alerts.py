import json
import urllib.request

import logging
import time

import os
from dotenv import load_dotenv
load_dotenv()

log = logging.getLogger("alerts")

# Paste a Discord channel's webhook URL here (Channel settings -> Integrations -> Webhooks).
# Leave it empty and alerts are only written to the log.
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
# at most one anomaly message per this many seconds; anomalies in between are summarised
ANOMALY_ALERT_INTERVAL_SECONDS = float(os.getenv("ANOMALY_ALERT_INTERVAL_SECONDS", "60"))

# The service levels this pipeline promises (also on the dashboard and in Grafana)
SLO_COMPLETENESS_PCT = float(os.getenv("SLO_COMPLETENESS_PCT", "99.9"))
SLO_LATENCY_P95_SECONDS = float(os.getenv("SLO_LATENCY_P95_SECONDS", "2"))
SLO_CONSUMER_LAG = int(os.getenv("SLO_CONSUMER_LAG", "1000"))


def send(text):
    """Post a message to Discord. Never raises: a broken webhook must not break the API."""
    if not DISCORD_WEBHOOK_URL:
        log.info(f"[alert, no webhook set] {text}")
        return False
    request = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=json.dumps({"content": text[:1900]}).encode(),  # Discord's limit is 2,000 characters
        headers={"Content-Type": "application/json", "User-Agent": "crypto-streaming-platform"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5):
            return True
    except Exception as e:
        log.warning(f"Could not send alert to Discord: {e}")
        return False


def describe_anomaly(row):
    kind = {"both": "PRICE + SIZE", "price": "PRICE", "volume": "SIZE"}.get(row["anomaly_type"], "ANOMALY")
    details = []
    if row.get("price_z") is not None:
        details.append(f"price z {row['price_z']:+.1f}")
    if row.get("size_ratio") is not None:
        details.append(f"size {row['size_ratio']:.0f}x median")
    return f"{kind} {row['symbol']} #{row['trade_id']} @ {row['price']} ({', '.join(details)})"


def slo_breaches(health):
    """Which promises the pipeline is currently breaking, as {name: description}."""
    breaches = {}
    for symbol in health.get("completeness", []):
        if symbol["completeness_pct"] < SLO_COMPLETENESS_PCT:
            breaches[f"completeness:{symbol['symbol']}"] = (
                f"{symbol['symbol']} completeness {symbol['completeness_pct']:.2f}% "
                f"(target {SLO_COMPLETENESS_PCT}%)")
    p95 = (health.get("latency_ms") or {}).get("p95")
    if p95 is not None and p95 / 1000 > SLO_LATENCY_P95_SECONDS:
        breaches["latency"] = f"p95 latency {p95 / 1000:.1f}s (target {SLO_LATENCY_P95_SECONDS:g}s)"
    for lag in health.get("consumer_lag", []):
        if lag["messages"] > SLO_CONSUMER_LAG:
            breaches[f"lag:{lag['group']}"] = (
                f"{lag['group']} is {lag['messages']} messages behind (target < {SLO_CONSUMER_LAG})")
    return breaches


class Alerter:
    """Remembers what it already said, so each problem is reported once, plus once when it clears."""

    def __init__(self, sender=send, clock=time.time):
        self.sender = sender
        self.clock = clock
        self.open_breaches = {}
        self.pending_anomalies = []
        self.last_anomaly_alert = 0.0

    def anomalies(self, rows):
        self.pending_anomalies.extend(rows)
        if not self.pending_anomalies:
            return
        if self.clock() - self.last_anomaly_alert < ANOMALY_ALERT_INTERVAL_SECONDS:
            return  # wait, then send one summary instead of a message per anomaly
        rows, self.pending_anomalies = self.pending_anomalies, []
        lines = [describe_anomaly(r) for r in rows[:5]]
        if len(rows) > 5:
            lines.append(f"...and {len(rows) - 5} more")
        title = "🚨 1 anomaly" if len(rows) == 1 else f"🚨 {len(rows)} anomalies"
        self.sender(title + "\n" + "\n".join(lines))
        self.last_anomaly_alert = self.clock()

    def health(self, health):
        breaches = slo_breaches(health)
        for name, text in breaches.items():
            if name not in self.open_breaches:
                self.sender(f"⚠️ SLO breach: {text}")
        for name in self.open_breaches:
            if name not in breaches:
                self.sender(f"✅ Recovered: {name.replace(':', ' ')} is back within its target")
        self.open_breaches = breaches
