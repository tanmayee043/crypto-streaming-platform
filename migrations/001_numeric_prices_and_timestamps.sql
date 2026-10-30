-- Week 2: store prices exactly, keep receive/store times for latency, add size_ratio.
-- Safe to run more than once.
ALTER TABLE trades ALTER COLUMN price TYPE NUMERIC(20, 8);
ALTER TABLE trades ALTER COLUMN quantity TYPE NUMERIC(20, 8);
ALTER TABLE trades ADD COLUMN IF NOT EXISTS size_ratio DOUBLE PRECISION;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS stored_at TIMESTAMPTZ;
