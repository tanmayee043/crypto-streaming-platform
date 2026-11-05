from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
import psycopg2.extras
import os
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, generate_latest

import asyncio
import json
import logging
import time
from collections import Counter as Tally, deque
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import alerts
import anomaly_detector
import gap_checker
from rate_limit import RateLimitMiddleware
from service_setup import postgres_args, settings
from scripts.replay import load_trades, send_times

load_dotenv()

log = logging.getLogger("api")

ROOT = Path(__file__).resolve().parent
# the only folders recorded trade files may be replayed from
REPLAY_DIRS = [ROOT / "replays", ROOT / "data", ROOT / "tests" / "data"]
POLL_SECONDS = 0.5      # how often the live stream checks Postgres for new rows
HEALTH_SECONDS = 5      # how often pipeline health is recomputed and pushed
KEEPALIVE_SECONDS = 15  # comment line so proxies don't close an idle stream

# Limits for the public site: a free VM has to survive visitors (and bots)
RATE_LIMIT_PER_MINUTE = settings.rate_limit_per_minute  # per IP, 0 turns it off
MAX_STREAMS_PER_IP = settings.max_streams_per_ip
MAX_REPLAYS_PER_IP = settings.max_replays_per_ip
MAX_REPLAYS_TOTAL = settings.max_replays_total  # replays use the most CPU

# API metrics live in their own registry, so importing anomaly_detector (for replay mode)
# doesn't make the API publish the detector's metrics as well
API_METRICS = CollectorRegistry()
STREAM_CLIENTS = Gauge("api_stream_clients", "Open live-stream connections", registry=API_METRICS)
ALERTS_SENT = Counter("api_alerts_total", "Alerts sent", ["kind"], registry=API_METRICS)


def get_conn():
    return psycopg2.connect(**postgres_args(), cursor_factory=psycopg2.extras.RealDictCursor)


def query(sql, params=()):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def to_json(value):
    """json.dumps helper for values Postgres hands back."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Can't encode {type(value).__name__}")


def sse(event, data, event_id=None):
    """One Server-Sent Event, in the text format browsers' EventSource understands."""
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {json.dumps(data, default=to_json)}\n\n"


TRADE_COLUMNS = ("id, symbol, trade_id, trade_time, price, quantity, price_z, size_ratio, volume_z, "
                 "is_anomaly, anomaly_type, received_at, stored_at")


# --- live stream: one loop reads new rows and hands them to every open connection ---

subscribers: set = set()
latest_health = None


def rows_after(last_id, symbol=None, limit=1000):
    return query(f"SELECT {TRADE_COLUMNS} FROM trades WHERE id > %s AND (%s::text IS NULL OR symbol = %s) "
                 "ORDER BY id LIMIT %s", (last_id, symbol, symbol, limit))


def publish(kind, payload):
    for queue in list(subscribers):
        try:
            queue.put_nowait((kind, payload))
        except asyncio.QueueFull:
            subscribers.discard(queue)  # a client this far behind gets disconnected; it reconnects and catches up


def untraced():
    """The background loop polls Postgres twice a second; tracing that would bury the real
    request traces in Jaeger, so the loop runs with tracing switched off."""
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return nullcontext()
    from opentelemetry.instrumentation.utils import suppress_instrumentation
    return suppress_instrumentation()


async def broadcast_once(alerter, last_id, last_health):
    """One round of the live loop. Returns the new (last_id, last_health)."""
    global latest_health
    if last_id is None:
        last_id = (await asyncio.to_thread(query, "SELECT coalesce(max(id), 0) AS id FROM trades"))[0]["id"]
    rows = await asyncio.to_thread(rows_after, last_id)
    if rows:
        last_id = rows[-1]["id"]
        publish("trades", rows)
    await asyncio.to_thread(alerter.anomalies, [r for r in rows if r["is_anomaly"]])
    if time.time() - last_health >= HEALTH_SECONDS:
        last_health = time.time()
        latest_health = await asyncio.to_thread(pipeline_health)
        publish("health", latest_health)
        await asyncio.to_thread(alerter.health, latest_health)
    return last_id, last_health


async def broadcaster():
    alerter = alerts.Alerter(sender=lambda text: alerts.send(text) and ALERTS_SENT.labels("discord").inc())  # type: ignore[func-returns-value]
    last_id, last_health = None, 0.0
    while True:
        try:
            with untraced():  # asyncio.to_thread copies this setting into the worker threads
                last_id, last_health = await broadcast_once(alerter, last_id, last_health)
        except Exception as e:  # e.g. Postgres restarting: keep the loop alive and try again
            log.warning(f"Live stream loop error: {e}")
        await asyncio.sleep(POLL_SECONDS)


@asynccontextmanager
async def lifespan(app):
    postgres_args()  # stop at startup, not on the first request, if a POSTGRES_* variable is missing
    task = asyncio.create_task(broadcaster())
    yield
    task.cancel()


app = FastAPI(title="Crypto Market Streaming Platform", lifespan=lifespan)
app.add_middleware(RateLimitMiddleware, per_minute=RATE_LIMIT_PER_MINUTE)

open_streams: Tally = Tally()  # client IP -> open live streams
open_replays: Tally = Tally()  # client IP -> running replays


def client_ip(request):
    return request.client.host if request.client else "unknown"


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse("/dashboard/")

# Traces go to any OpenTelemetry collector (e.g. Jaeger: docker compose --profile observability up).
# Without OTEL_EXPORTER_OTLP_ENDPOINT set, tracing stays off and costs nothing.
if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": "crypto-api"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    Psycopg2Instrumentor().instrument(skip_dep_check=True)
    # long-lived streams would show up as one endless span, so they're left out
    FastAPIInstrumentor.instrument_app(app, excluded_urls="stream,metrics,dashboard")


@app.get("/stream")
async def stream(request: Request, symbol: str | None = None, after_id: int | None = None):
    """Live Server-Sent Events: `trades` (new rows for `symbol`), `anomaly` (any symbol) and `health`.
    `after_id` (or the browser's automatic Last-Event-ID on reconnect) replays rows it missed."""
    last_event = request.headers.get("last-event-id")
    after_id = int(last_event) if last_event and last_event.isdigit() else after_id
    ip = client_ip(request)
    if open_streams[ip] >= MAX_STREAMS_PER_IP:
        raise HTTPException(429, detail="Too many open streams from this address.")

    async def events():
        queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        subscribers.add(queue)  # subscribe BEFORE catching up, so nothing slips between the two
        STREAM_CLIENTS.inc()
        open_streams[ip] += 1
        sent_id = after_id
        try:
            yield "retry: 3000\n\n"  # browser reconnects after 3 s if the connection drops
            if after_id is not None:
                missed = await asyncio.to_thread(rows_after, after_id, symbol, 500)
                if missed:
                    sent_id = missed[-1]["id"]
                    yield sse("trades", missed, sent_id)
            if latest_health:
                yield sse("health", latest_health)
            while not await request.is_disconnected():
                try:
                    kind, payload = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                if kind != "trades":
                    yield sse(kind, payload)
                    continue
                fresh = [r for r in payload if sent_id is None or r["id"] > sent_id]
                for row in fresh:
                    if row["is_anomaly"]:
                        yield sse("anomaly", row)
                mine = [r for r in fresh if symbol is None or r["symbol"] == symbol]
                if fresh:
                    sent_id = fresh[-1]["id"]
                if mine:
                    yield sse("trades", mine, sent_id)
        finally:
            subscribers.discard(queue)
            STREAM_CLIENTS.dec()
            open_streams[ip] -= 1

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# --- pipeline health ---

def pipeline_health():
    now = time.time()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # stored_at and received_at both come from this machine's clock, so compare them with
            # Python's time, never Postgres' now() (the database container's clock can differ)
            cur.execute("SELECT symbol, count(*) AS n FROM trades WHERE stored_at > to_timestamp(%s) "
                        "GROUP BY symbol", (now - 60,))
            per_symbol = {r["symbol"]: round(r["n"] / 60, 2) for r in cur.fetchall()}
            cur.execute("""
                SELECT count(*) AS n,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95
                FROM (SELECT extract(epoch FROM stored_at - received_at) * 1000 AS ms FROM trades
                      WHERE stored_at > to_timestamp(%s) AND received_at IS NOT NULL) t""", (now - 300,))
            latency = cur.fetchone()
    finally:
        conn.close()
    plain = gap_checker.connect_postgres()  # gap_checker's queries expect plain tuple rows
    try:
        freshness = gap_checker.freshness(plain)
        completeness = gap_checker.completeness(plain, 24)
    finally:
        plain.close()
    try:
        lag = [{"group": g, "topic": t, "messages": n} for (g, t), n in gap_checker.consumer_lag().items()]
        lag_error = None
    except Exception as e:
        lag, lag_error = [], str(e)
    health = {
        "checked_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "trades_per_second": round(sum(per_symbol.values()), 2),
        "trades_per_second_by_symbol": per_symbol,
        "latency_ms": {"samples": latency["n"],
                       "p50": round(latency["p50"], 1) if latency["p50"] is not None else None,
                       "p95": round(latency["p95"], 1) if latency["p95"] is not None else None},
        "seconds_since_last_trade": freshness,
        "completeness": completeness,
        "consumer_lag": lag,
        "consumer_lag_error": lag_error,
        "slo": {"completeness_pct": alerts.SLO_COMPLETENESS_PCT,
                "latency_p95_seconds": alerts.SLO_LATENCY_P95_SECONDS,
                "consumer_lag": alerts.SLO_CONSUMER_LAG},
    }
    breaches = alerts.slo_breaches(health)
    health["breaches"] = list(breaches.values())
    health["status"] = "degraded" if breaches or lag_error else "ok"
    return health


@app.get("/health/pipeline")
def get_pipeline_health():
    return latest_health or pipeline_health()


@app.get("/healthz")
def healthz():
    """Liveness: the API process is up."""
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    """Readiness: the API can reach Postgres."""
    try:
        query("SELECT 1")
    except psycopg2.Error as e:
        raise HTTPException(503, detail=f"database unavailable: {e.__class__.__name__}") from e
    return {"status": "ready"}


# --- trades and anomalies ---

# `symbol` is optional on every endpoint: leave it out to get all symbols
@app.get("/anomalies")
def get_anomalies(limit: int = Query(50, ge=1, le=500), symbol: str | None = None,
                  type: str | None = Query(None, pattern="^(price|volume|both)$"),
                  start: datetime | None = None, end: datetime | None = None,
                  before_id: int | None = None):
    """Newest first. For the next page, pass the last row's `id` as `before_id` (cursor pagination)."""
    return query(
        "SELECT * FROM trades WHERE is_anomaly = TRUE "
        "AND (%s::text IS NULL OR symbol = %s) AND (%s::text IS NULL OR anomaly_type = %s) "
        "AND (%s::timestamptz IS NULL OR trade_time >= %s) AND (%s::timestamptz IS NULL OR trade_time < %s) "
        "AND (%s::bigint IS NULL OR id < %s) "
        "ORDER BY id DESC LIMIT %s",
        (symbol, symbol, type, type, start, start, end, end, before_id, before_id, limit))

@app.get("/trades")
def get_trades(limit: int = Query(100, ge=1, le=1000), symbol: str | None = None):
    return query("SELECT * FROM trades WHERE (%s::text IS NULL OR symbol = %s) "
                 "ORDER BY trade_time DESC, id DESC LIMIT %s", (symbol, symbol, limit))

@app.get("/anomalies/count")
def get_anomaly_count(symbol: str | None = None,
                      type: str | None = Query(None, pattern="^(price|volume|both)$"),
                      start: datetime | None = None, end: datetime | None = None):
    return query(
        "SELECT COUNT(*) AS count FROM trades WHERE is_anomaly = TRUE "
        "AND (%s::text IS NULL OR symbol = %s) AND (%s::text IS NULL OR anomaly_type = %s) "
        "AND (%s::timestamptz IS NULL OR trade_time >= %s) AND (%s::timestamptz IS NULL OR trade_time < %s)",
        (symbol, symbol, type, type, start, start, end, end))[0]

@app.get("/symbols")
def get_symbols():
    return query("SELECT symbol, count(*) AS trades, max(trade_time) AS last_trade FROM trades "
                 "GROUP BY symbol ORDER BY symbol")


def explain(row, previous_price):
    """Plain-English reasons for the analyst, from the stored scores and current thresholds."""
    if row["volume_z"] is not None and row["size_ratio"] is None:
        # stored by the original (v1) model, whose scores mean something different
        return [{"signal": "v1 model", "flagged": row["is_anomaly"],
                 "text": f"Scored by the original model: price z-score {row['price_z']:.2f}, volume z-score "
                         f"{row['volume_z']:.2f} (mean/stdev over 30 trades, flagged above 3)."}]
    reasons = []
    if row["price_z"] is not None and previous_price:
        move = (float(row["price"]) / float(previous_price) - 1) * 100
        reasons.append({
            "signal": "price",
            "flagged": abs(row["price_z"]) > anomaly_detector.PRICE_Z_THRESHOLD,
            "text": f"Price moved {move:+.3f}% from the previous trade: {abs(row['price_z']):.1f} robust standard "
                    f"deviations from the typical move of the last {anomaly_detector.WINDOW_SIZE} trades "
                    f"(flagged above {anomaly_detector.PRICE_Z_THRESHOLD:g}).",
        })
    if row["size_ratio"] is not None:
        reasons.append({
            "signal": "size",
            "flagged": row["size_ratio"] >= anomaly_detector.SIZE_RATIO_THRESHOLD,
            "text": f"Trade size was {row['size_ratio']:.1f}x the median of the last "
                    f"{anomaly_detector.WINDOW_SIZE} trades (flagged at {anomaly_detector.SIZE_RATIO_THRESHOLD:g}x).",
        })
    if not reasons:
        reasons.append({"signal": "warm-up", "flagged": False,
                        "text": "Not scored: this symbol's window was still filling up after a start."})
    return reasons


@app.get("/anomalies/{symbol}/{trade_id}")
def get_anomaly_detail(symbol: str, trade_id: int, before: int = Query(30, ge=0, le=200),
                       after: int = Query(10, ge=0, le=200)):
    """One trade with the trades around it (for the mini chart) and why it was or wasn't flagged."""
    rows = query("SELECT * FROM trades WHERE symbol = %s AND trade_id = %s", (symbol, trade_id))
    if not rows:
        raise HTTPException(404, detail=f"No trade {trade_id} for {symbol}")
    earlier = query(f"SELECT {TRADE_COLUMNS} FROM trades WHERE symbol = %s AND trade_id < %s "
                    "ORDER BY trade_id DESC LIMIT %s", (symbol, trade_id, before))
    later = query(f"SELECT {TRADE_COLUMNS} FROM trades WHERE symbol = %s AND trade_id > %s "
                  "ORDER BY trade_id LIMIT %s", (symbol, trade_id, after))
    trade = rows[0]
    previous_price = earlier[0]["price"] if earlier and earlier[0]["trade_id"] == trade_id - 1 else None
    return {"trade": trade, "context": list(reversed(earlier)) + [trade] + later,
            "reasons": explain(trade, previous_price)}


@app.get("/completeness")
def get_completeness(hours: float = 24):
    """Per symbol: how many of the trade IDs in range we actually stored, plus the gaps."""
    conn = gap_checker.connect_postgres()
    report = gap_checker.completeness(conn, hours)
    gaps = gap_checker.find_gaps(conn, hours)
    conn.close()
    for r in report:
        r["gaps"] = [{"first_missing_id": first, "last_missing_id": last}
                     for symbol, first, last in gaps if symbol == r["symbol"]][:50]
    return report


# --- replay mode: the same model, run in this process on a recorded file ---

_replay_files: dict = {}  # path -> (modified time, summary)


def describe_file(path):
    mtime = path.stat().st_mtime
    if path in _replay_files and _replay_files[path][0] == mtime:
        return _replay_files[path][1]
    trades = load_trades(path)
    summary = {"name": path.name, "trades": len(trades),
               "symbols": sorted({t["s"] for t in trades}),
               "start": datetime.fromtimestamp(trades[0]["T"] / 1000, tz=timezone.utc).isoformat(),
               "end": datetime.fromtimestamp(trades[-1]["T"] / 1000, tz=timezone.utc).isoformat(),
               "hours": round((trades[-1]["T"] - trades[0]["T"]) / 3_600_000, 2)} if trades else None
    _replay_files[path] = (mtime, summary)
    return summary


def replay_paths():
    return {p.name: p for folder in REPLAY_DIRS if folder.exists() for p in sorted(folder.glob("*.jsonl"))}


@app.get("/replay/files")
def get_replay_files():
    return [s for s in (describe_file(p) for p in replay_paths().values()) if s]


@app.get("/replay/stream")
async def replay_stream(request: Request, file: str, speed: float = Query(100, ge=1, le=1000),
                        symbol: str | None = None):
    """Replays a recorded file through the anomaly model at `speed`x, as Server-Sent Events
    (`trades`, `anomaly`, `progress`, `done`). Nothing is written to Kafka or Postgres."""
    path = replay_paths().get(file)  # only files from the replay folders, never an arbitrary path
    if path is None:
        raise HTTPException(404, detail=f"No recorded file called {file}")
    ip = client_ip(request)
    if open_replays[ip] >= MAX_REPLAYS_PER_IP or sum(open_replays.values()) >= MAX_REPLAYS_TOTAL:
        raise HTTPException(429, detail="Too many replays are running. Please try again in a minute.")
    trades = await asyncio.to_thread(load_trades, path)

    async def events():
        open_replays[ip] += 1
        try:
            async for chunk in replay_events(request, trades, speed, symbol):
                yield chunk
        finally:
            open_replays[ip] -= 1

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


async def replay_events(request, trades, speed, symbol):
    """The replay itself: score each trade on schedule and yield Server-Sent Events."""
    windows: dict = {}
    recent: dict = {}  # symbol -> last 30 replayed rows, sent with each anomaly as its context
    schedule = send_times(trades, speed=speed)
    started = time.monotonic()
    batch: list = []
    flagged: dict = {}
    last_flush = time.monotonic()
    for i, (trade, due) in enumerate(zip(trades, schedule, strict=True)):
        wait = due - (time.monotonic() - started)
        if wait > 0:
            await asyncio.sleep(wait)
        window = windows.setdefault(trade["s"], anomaly_detector.new_window())
        previous_price = window["last_price"]
        price_z, size_ratio = anomaly_detector.score_trade(window, float(trade["p"]), float(trade["q"]))
        anomaly_type = anomaly_detector.classify_anomaly(price_z, size_ratio)
        row = {"id": i, "symbol": trade["s"], "trade_id": trade["t"],
               "trade_time": datetime.fromtimestamp(trade["T"] / 1000, tz=timezone.utc).isoformat(),
               "price": float(trade["p"]), "quantity": float(trade["q"]), "price_z": price_z,
               "size_ratio": size_ratio, "volume_z": None, "is_anomaly": anomaly_type is not None,
               "anomaly_type": anomaly_type}
        context = recent.setdefault(trade["s"], deque(maxlen=30))
        if anomaly_type:
            flagged[anomaly_type] = flagged.get(anomaly_type, 0) + 1
            yield sse("anomaly", {**row, "reasons": explain(row, previous_price),
                                  "context": [*context, row]})
        context.append(row)
        if symbol is None or trade["s"] == symbol:
            batch.append(row)
        # group trades into ~10 messages a second, so 1000x speed doesn't flood the browser
        if time.monotonic() - last_flush >= 0.1 or i == len(trades) - 1:
            if await request.is_disconnected():
                return
            if batch:
                yield sse("trades", batch)
            yield sse("progress", {"sent": i + 1, "total": len(trades), "market_time": row["trade_time"]})
            batch, last_flush = [], time.monotonic()
    yield sse("done", {"trades": len(trades), "anomalies": flagged,
                       "seconds": round(time.monotonic() - started, 1)})


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(API_METRICS), media_type=CONTENT_TYPE_LATEST)


app.mount("/dashboard", StaticFiles(directory=ROOT / "static", html=True), name="dashboard")
