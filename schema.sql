-- Full current schema, used for fresh databases (docker compose runs it on a new volume).
-- Existing databases: apply the files in migrations/ in order instead.

CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    price NUMERIC(20, 8) NOT NULL,       -- exact decimals: money is never a float
    quantity NUMERIC(20, 8) NOT NULL,
    price_z DOUBLE PRECISION,            -- robust z-score of the log return (NULL while warming up)
    volume_z DOUBLE PRECISION,           -- v1 model only; new rows use size_ratio
    size_ratio DOUBLE PRECISION,         -- trade size / median size of the window
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_type TEXT,
    received_at TIMESTAMPTZ,             -- when our producer received the trade (our clock)
    stored_at TIMESTAMPTZ,               -- when the storage consumer wrote it (same clock)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, trade_time),
    UNIQUE (symbol, trade_id, trade_time)   -- a trade's exchange timestamp never changes
) PARTITION BY RANGE (trade_time);          -- one partition per day; retention.py creates and drops them

-- Catches anything outside the daily partitions (e.g. before retention.py first runs)
CREATE TABLE IF NOT EXISTS trades_default PARTITION OF trades DEFAULT;

-- Missing trade-ID ranges found by gap_checker.py
CREATE TABLE IF NOT EXISTS trade_gaps (
    symbol TEXT NOT NULL,
    first_missing_id BIGINT NOT NULL,
    last_missing_id BIGINT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,             -- set once the missing trades have been filled in
    PRIMARY KEY (symbol, first_missing_id)
);
