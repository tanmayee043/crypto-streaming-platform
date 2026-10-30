"""Shared fixtures: a throwaway Postgres container for the whole integration test run."""

from pathlib import Path

import psycopg2
import pytest

SCHEMA = (Path(__file__).resolve().parents[2] / "schema.sql").read_text()


@pytest.fixture(scope="session")
def postgres_url():
    postgres = pytest.importorskip("testcontainers.postgres")
    try:
        container = postgres.PostgresContainer("postgres:16.15", driver=None)
        container.start()
    except Exception as e:  # Docker not running
        pytest.skip(f"Docker unavailable: {e}")
    try:
        yield container.get_connection_url()
    finally:
        container.stop()


@pytest.fixture
def conn(postgres_url):
    connection = psycopg2.connect(postgres_url)
    with connection, connection.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS trades, trade_gaps")
        cur.execute(SCHEMA)
    yield connection
    connection.close()


def count_rows(conn, table="trades"):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]
