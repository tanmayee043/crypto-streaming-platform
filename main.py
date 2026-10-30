from fastapi import FastAPI
import psycopg2.extras
import os
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

import gap_checker

load_dotenv()

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

# `symbol` is optional on every endpoint: leave it out to get all symbols
@app.get("/anomalies")
def get_anomalies(limit: int = 50, symbol: str | None = None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades WHERE is_anomaly = TRUE AND (%s::text IS NULL OR symbol = %s) "
                "ORDER BY trade_time DESC LIMIT %s", (symbol, symbol, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/trades")
def get_trades(limit: int = 100, symbol: str | None = None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades WHERE (%s::text IS NULL OR symbol = %s) "
                "ORDER BY trade_time DESC LIMIT %s", (symbol, symbol, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/anomalies/count")
def get_anomaly_count(symbol: str | None = None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM trades WHERE is_anomaly = TRUE AND (%s::text IS NULL OR symbol = %s)",
                (symbol, symbol))
    result = cur.fetchone()
    conn.close()
    return result

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

app.mount("/dashboard", StaticFiles(directory="static", html=True), name="dashboard")
