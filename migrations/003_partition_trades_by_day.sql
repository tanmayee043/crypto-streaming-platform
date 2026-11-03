-- Week 4: partition trades by day, so old data is removed by dropping a whole partition
-- (instant, and gives the disk space back) instead of slow row-by-row deletes.
-- Postgres requires unique keys on a partitioned table to include the partition column, so
-- the upsert key becomes (symbol, trade_id, trade_time). A trade's exchange timestamp never
-- changes, so a redelivered trade still hits the same row.
-- Safe to run more than once: it does nothing if trades is already partitioned.
DO $$
DECLARE
    day date;
BEGIN
    IF (SELECT relkind FROM pg_class WHERE oid = 'trades'::regclass) = 'p' THEN
        RETURN;
    END IF;

    ALTER TABLE trades RENAME TO trades_unpartitioned;

    CREATE TABLE trades (
        id BIGSERIAL,
        symbol TEXT NOT NULL,
        trade_id BIGINT NOT NULL,
        trade_time TIMESTAMPTZ NOT NULL,
        price NUMERIC(20, 8) NOT NULL,
        quantity NUMERIC(20, 8) NOT NULL,
        price_z DOUBLE PRECISION,
        volume_z DOUBLE PRECISION,
        size_ratio DOUBLE PRECISION,
        is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
        anomaly_type TEXT,
        received_at TIMESTAMPTZ,
        stored_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (id, trade_time),
        UNIQUE (symbol, trade_id, trade_time)
    ) PARTITION BY RANGE (trade_time);

    CREATE TABLE trades_default PARTITION OF trades DEFAULT;

    FOR day IN SELECT DISTINCT (trade_time AT TIME ZONE 'UTC')::date FROM trades_unpartitioned LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF trades FOR VALUES FROM (%L) TO (%L)',
            'trades_' || to_char(day, 'YYYYMMDD'),
            day::timestamp AT TIME ZONE 'UTC',
            (day + 1)::timestamp AT TIME ZONE 'UTC');
    END LOOP;

    INSERT INTO trades (id, symbol, trade_id, trade_time, price, quantity, price_z, volume_z, size_ratio,
                        is_anomaly, anomaly_type, received_at, stored_at, created_at)
    SELECT id, symbol, trade_id, trade_time, price, quantity, price_z, volume_z, size_ratio,
           is_anomaly, anomaly_type, received_at, stored_at, created_at
    FROM trades_unpartitioned;

    PERFORM setval(pg_get_serial_sequence('trades', 'id'),
                   greatest((SELECT coalesce(max(id), 0) FROM trades), 1));
    DROP TABLE trades_unpartitioned;
END $$;
