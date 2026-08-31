from fastapi import FastAPI
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

@app.get("/anomalies")
def get_anomalies(limit: int = 50):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades WHERE is_anomaly = TRUE ORDER BY trade_time DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/trades")
def get_trades(limit: int = 100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades ORDER BY trade_time DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows