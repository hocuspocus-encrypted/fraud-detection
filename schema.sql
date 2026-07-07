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
