"""What every service does at startup: check its settings, set up logging, handle shutdown."""

import json
import logging
import signal
import threading
from datetime import datetime, timezone
from typing import Literal

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()


class Settings(BaseSettings):
    """Environment variables with a default. A bad value stops the service at startup and names the variable."""

    model_config = SettingsConfigDict(extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["text", "json"] = "text"  # text for a terminal, json for log tools
    kafka_bootstrap_servers: str = Field("localhost:9092", min_length=1)
    metrics_interval_seconds: float = Field(60, gt=0)

    # anomaly detector
    window_size: int = Field(100, ge=2)
    price_z_threshold: float = Field(6.0, gt=0)
    size_ratio_threshold: float = Field(50, gt=0)
    detector_batch_size: int = Field(500, ge=1)
    rebuild_lookback: int = Field(2000, ge=0)

    # storage consumer
    storage_batch_size: int = Field(500, ge=1)
    storage_batch_timeout_seconds: float = Field(1.0, gt=0)

    # gap checker and retention
    gap_check_interval_seconds: float = Field(15, gt=0)
    gap_check_lookback_hours: float = Field(24, gt=0)
    retention_days: int = Field(90, ge=1)
    retention_interval_seconds: float = Field(3600, gt=0)

    # API limits and alerts
    rate_limit_per_minute: int = Field(300, ge=0)  # 0 turns it off
    max_streams_per_ip: int = Field(5, ge=1)
    max_replays_per_ip: int = Field(2, ge=1)
    max_replays_total: int = Field(5, ge=1)  # replays use the most CPU
    anomaly_alert_interval_seconds: float = Field(60, gt=0)
    slo_completeness_pct: float = Field(99.9, gt=0, le=100)
    slo_latency_p95_seconds: float = Field(2, gt=0)
    slo_consumer_lag: int = Field(1000, ge=0)


class PostgresSettings(BaseSettings):
    """POSTGRES_* variables. Read when connecting, because importing a module needs no database."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")

    host: str = "localhost"
    port: int = Field(ge=1, le=65535)
    db: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: str = Field(min_length=1)


settings = Settings()


def postgres_args():
    """Arguments for psycopg2.connect. Raises with every missing or bad POSTGRES_* variable named."""
    postgres = PostgresSettings()
    return {
        "host": postgres.host,
        "port": postgres.port,
        "dbname": postgres.db,
        "user": postgres.user,
        "password": postgres.password,
    }


class JsonFormatter(logging.Formatter):
    """One JSON object per line, so log tools can filter by level or logger."""

    def format(self, record):
        entry = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging():
    handler = logging.StreamHandler()
    if settings.log_format == "json":
        handler.setFormatter(JsonFormatter())
        # uvicorn sets up its own text loggers before it imports the app; send them through ours
        for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
            logging.getLogger(name).handlers.clear()
            logging.getLogger(name).propagate = True
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.basicConfig(level=settings.log_level, handlers=[handler])


# --- shutdown: `docker stop` sends SIGTERM, then kills the process 10 seconds later ---

stop_requested = threading.Event()


def stop_after_batch_on_sigterm():
    """For the consumers: finish and commit the batch in progress, then leave the loop."""
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop_requested.set())


def exit_on_sigterm():
    """For services with no batch to finish: run the same cleanup as Ctrl+C."""
    signal.signal(signal.SIGTERM, signal.default_int_handler)
