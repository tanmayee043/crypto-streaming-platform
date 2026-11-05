"""Settings checks, JSON logs and what each service does when Docker asks it to stop."""

import json
import logging
import signal

import pytest
from pydantic import ValidationError

import anomaly_detector as ad
import service_setup
import storage_consumer as sc
from service_setup import JsonFormatter, PostgresSettings, Settings, postgres_args
from tests.conftest import FakeMessage, FakeProducer, make_record

POSTGRES_VARS = {"POSTGRES_HOST": "db", "POSTGRES_PORT": "5433", "POSTGRES_DB": "crypto",
                 "POSTGRES_USER": "me", "POSTGRES_PASSWORD": "secret"}


# --- settings ---

@pytest.mark.parametrize("variable, value", [
    ("WINDOW_SIZE", "abc"),
    ("WINDOW_SIZE", "1"),
    ("STORAGE_BATCH_SIZE", "0"),
    ("RETENTION_DAYS", "0"),
    ("SLO_COMPLETENESS_PCT", "101"),
    ("LOG_FORMAT", "yaml"),
])
def test_bad_value_stops_startup_and_names_the_variable(monkeypatch, variable, value):
    monkeypatch.setenv(variable, value)
    with pytest.raises(ValidationError, match=variable.lower()):
        Settings()


def test_good_value_is_converted(monkeypatch):
    monkeypatch.setenv("WINDOW_SIZE", "250")
    assert Settings().window_size == 250


def test_postgres_args_come_from_the_environment(monkeypatch):
    for key, value in POSTGRES_VARS.items():
        monkeypatch.setenv(key, value)
    assert postgres_args() == {"host": "db", "port": 5433, "dbname": "crypto", "user": "me", "password": "secret"}


def test_missing_postgres_variable_is_named(monkeypatch):
    for key, value in POSTGRES_VARS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("POSTGRES_PASSWORD")
    with pytest.raises(ValidationError, match="password"):
        PostgresSettings()


# --- JSON logs ---

def test_json_log_line():
    record = logging.LogRecord("producer", logging.WARNING, __file__, 1, "lost %d trades", (3,), None)
    entry = json.loads(JsonFormatter().format(record))
    assert entry["level"] == "WARNING"
    assert entry["logger"] == "producer"
    assert entry["message"] == "lost 3 trades"
    assert entry["time"].endswith("+00:00")


def test_json_log_line_includes_the_exception():
    try:
        raise ValueError("bad trade")
    except ValueError:
        import sys
        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    assert "ValueError: bad trade" in json.loads(JsonFormatter().format(record))["exception"]


# --- shutdown ---

@pytest.fixture
def clean_shutdown_state():
    """Puts back the signal handler and clears the stop flag, so one test can't affect the next."""
    previous = signal.getsignal(signal.SIGTERM)
    service_setup.stop_requested.clear()
    yield
    signal.signal(signal.SIGTERM, previous)
    service_setup.stop_requested.clear()


def test_exit_on_sigterm_runs_the_ctrl_c_cleanup(clean_shutdown_state):
    service_setup.exit_on_sigterm()
    with pytest.raises(KeyboardInterrupt):
        signal.raise_signal(signal.SIGTERM)


class FakeConsumer:
    latest: "FakeConsumer"

    def __init__(self, _config):
        self.closed = False
        FakeConsumer.latest = self

    def subscribe(self, topics, **callbacks):
        pass

    def close(self):
        self.closed = True


class FakeConn:
    def close(self):
        pass


@pytest.mark.parametrize("module", [sc, ad], ids=["storage_consumer", "anomaly_detector"])
def test_sigterm_finishes_the_batch_in_progress_then_stops(monkeypatch, clean_shutdown_state, module):
    message = FakeMessage(json.dumps(make_record(1)).encode(), 0)
    polls, processed = [], []

    def fake_consume_batch(consumer, size, timeout):
        polls.append(1)
        assert len(polls) == 1, "must not read another batch after SIGTERM"
        signal.raise_signal(signal.SIGTERM)  # arrives while this batch is in progress
        return [message]

    def fake_process_batch(consumer, *rest):
        processed.append(message)
        assert not FakeConsumer.latest.closed, "the batch must finish before the consumer closes"
        return FakeConn()

    monkeypatch.setattr(module, "METRICS_PORT", 0)
    monkeypatch.setattr(module, "Consumer", FakeConsumer)
    monkeypatch.setattr(module, "Producer", lambda _config: FakeProducer())
    monkeypatch.setattr(module, "consume_batch", fake_consume_batch)
    monkeypatch.setattr(module, "process_batch", fake_process_batch)
    if module is sc:
        monkeypatch.setattr(module, "connect_postgres", FakeConn)

    module.main()

    assert processed == [message]
    assert FakeConsumer.latest.closed
