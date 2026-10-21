-- 0001 baseline: the pre-existing pipeline tables, copied from schema.sql so a fresh
-- database can be built from migrations alone. Idempotent on fraud_db, where they already exist.

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id     VARCHAR(64) PRIMARY KEY,
    event_timestamp    TIMESTAMPTZ NOT NULL,
    user_id            VARCHAR(32) NOT NULL,
    amount             NUMERIC(12, 2) NOT NULL,
    currency           VARCHAR(8) NOT NULL,
    merchant           VARCHAR(255),
    merchant_category  VARCHAR(64),
    card_type          VARCHAR(32),
    card_last4         VARCHAR(4),
    location_city      VARCHAR(128),
    location_country   VARCHAR(8),
    is_fraud           BOOLEAN NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Speeds up the queries you'll likely run later: per-user activity,
-- fraud rate over time, filtering by category.
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions (is_fraud);

-- Rolling-window anomaly flags from spark_fraud_detection.py.
-- One row per (user_id, window_start, window_end): if the same window gets
-- recomputed on a later trigger (more data arrived), it's upserted in place
-- rather than duplicated.
CREATE TABLE IF NOT EXISTS anomaly_flags (
    id              SERIAL PRIMARY KEY,
    user_id         VARCHAR(32) NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    txn_count       INTEGER NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_flags_user_id ON anomaly_flags (user_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_flags_window_start ON anomaly_flags (window_start);
