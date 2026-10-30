-- Week 2: missing trade-ID ranges found by gap_checker.py. Safe to run more than once.
CREATE TABLE IF NOT EXISTS trade_gaps (
    symbol TEXT NOT NULL,
    first_missing_id BIGINT NOT NULL,
    last_missing_id BIGINT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY (symbol, first_missing_id)
);
