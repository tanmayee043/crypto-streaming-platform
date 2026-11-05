"""Generates observability/grafana/dashboards/pipeline.json. Edit the panels here, then run:

    python observability/make_dashboard.py
"""

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "grafana" / "dashboards" / "pipeline.json"
DS = {"type": "prometheus", "uid": "prometheus"}
panels: list = []
next_id = 1


def panel(title, kind, targets, x, y, w, h, unit=None, description="", thresholds=None, decimals=None):
    global next_id
    p = {
        "id": next_id, "title": title, "type": kind, "datasource": DS, "description": description,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"datasource": DS, "expr": expr, "legendFormat": legend, "refId": chr(65 + i)}
                    for i, (expr, legend) in enumerate(targets)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
    if kind == "stat":
        p["fieldConfig"]["defaults"]["noValue"] = "Not reporting"  # says why, instead of "No data"
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "none"}
    next_id += 1
    panels.append(p)


def row(title, y):
    global next_id
    panels.append({"id": next_id, "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    next_id += 1


# NEUTRAL is the "no value" colour: a stopped service must not look like an alarm (red) or like
# "all good" (green), so every threshold list starts with it and real values begin at 0
GREEN, RED, NEUTRAL = "green", "red", "text"

row("Service levels", 0)
panel("Completeness (worst symbol, 24 h)", "stat", [("min(pipeline_completeness_ratio) * 100", "")], 0, 1, 6, 4,
      unit="percent", decimals=2, description="Stored trade IDs / expected trade IDs. Target: 99.9% or more.",
      thresholds=[{"color": NEUTRAL, "value": None}, {"color": RED, "value": 0}, {"color": GREEN, "value": 99.9}])
panel("End-to-end latency p95 (5 min)", "stat",
      [("histogram_quantile(0.95, sum by (le) (rate(pipeline_end_to_end_latency_seconds_bucket[5m])))", "")],
      6, 1, 6, 4, unit="s", description="Producer receipt to row stored. Target: 2 s or less.",
      thresholds=[{"color": NEUTRAL, "value": None}, {"color": GREEN, "value": 0}, {"color": RED, "value": 2}])
panel("Consumer lag (worst group)", "stat", [("max(pipeline_consumer_lag_messages)", "")], 12, 1, 6, 4,
      description="Messages not yet processed. Target: under 1,000.",
      thresholds=[{"color": NEUTRAL, "value": None}, {"color": GREEN, "value": 0}, {"color": RED, "value": 1000}])
panel("Services up", "stat", [("sum(up)", "answering"), ("count(up)", "configured")], 18, 1, 6, 4,
      description="Scraped services answering / services configured (producer, detector, storage, gap checker, API).")

row("Throughput", 5)
panel("Trades received per second", "timeseries", [("sum by (symbol) (rate(producer_trades_total[1m]))", "{{symbol}}")],
      0, 6, 12, 8, unit="ops")
panel("Rows written per second", "timeseries", [
    ("rate(storage_rows_inserted_total[1m])", "new rows"),
    ("rate(storage_rows_reupserted_total[1m])", "redeliveries absorbed by the upsert")], 12, 6, 12, 8, unit="ops")

row("Latency by stage", 14)
panel("Latency p50 / p95", "timeseries", [
    ("histogram_quantile(0.5, sum by (le) (rate(pipeline_end_to_end_latency_seconds_bucket[5m])))", "end to end p50"),
    ("histogram_quantile(0.95, sum by (le) (rate(pipeline_end_to_end_latency_seconds_bucket[5m])))", "end to end p95"),
], 0, 15, 12, 8, unit="s")
panel("Where the time goes (p95 per stage)", "timeseries", [
    ("histogram_quantile(0.95, sum by (le) (rate(detector_stage_latency_seconds_bucket[5m])))",
     "1. receipt -> detector result in Kafka"),
    ("histogram_quantile(0.95, sum by (le) (rate(storage_stage_latency_seconds_bucket[5m])))",
     "2. result in Kafka -> row stored"),
    ("histogram_quantile(0.95, sum by (le) (rate(storage_write_seconds_bucket[5m])))", "Postgres batch write"),
    ("histogram_quantile(0.95, sum by (le) (rate(detector_batch_seconds_bucket[5m])))", "detector batch"),
], 12, 15, 12, 8, unit="s",
      description="Splits end-to-end latency into the detector stage and the storage stage, "
                  "to find which one adds delay.")

row("Backlog and completeness", 23)
panel("Consumer lag", "timeseries", [("pipeline_consumer_lag_messages", "{{group}}")], 0, 24, 8, 8)
panel("Completeness by symbol", "timeseries", [("pipeline_completeness_ratio * 100", "{{symbol}}")], 8, 24, 8, 8,
      unit="percent")
panel("Seconds since newest stored trade", "timeseries", [("pipeline_seconds_since_last_trade", "{{symbol}}")],
      16, 24, 8, 8, unit="s")

row("Anomalies and errors", 32)
panel("Anomalies per minute", "timeseries", [("sum by (type) (increase(detector_anomalies_total[1m]))", "{{type}}")],
      0, 33, 12, 8)
panel("Errors (last 5 min)", "timeseries", [
    ("increase(detector_dead_letters_total[5m])", "detector dead letters"),
    ("increase(storage_dead_letters_total[5m])", "storage dead letters"),
    ("increase(storage_write_retries_total[5m])", "Postgres write retries"),
    ("increase(producer_reconnects_total[5m])", "WebSocket reconnects"),
    ("increase(producer_delivery_failures_total[5m])", "Kafka delivery failures"),
], 12, 33, 12, 8)

row("Disk", 41)
panel("Database size", "timeseries", [("pipeline_database_size_bytes", "Postgres")], 0, 42, 12, 8, unit="bytes",
      description="Retention drops daily partitions older than RETENTION_DAYS, so this should level off.")
panel("Daily partitions", "stat", [("pipeline_trade_partitions", "")], 12, 42, 12, 8)

dashboard = {
    "uid": "crypto-pipeline",
    "title": "Crypto streaming pipeline",
    "description": "Throughput, per-stage latency, consumer lag, completeness and errors. SLOs: completeness >= 99.9%, "
                   "p95 latency <= 2 s, consumer lag < 1,000.",
    "tags": ["crypto", "kafka", "slo"],
    "timezone": "browser",
    "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"},
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "panels": panels,
}
OUT.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {OUT} with {len(panels)} panels")
