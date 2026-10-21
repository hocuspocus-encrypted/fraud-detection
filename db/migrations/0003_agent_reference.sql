-- 0003 reference data the agent tools read, and the agent's view of transactions
-- (docs/CURRENT_STATE.md gaps G3, G5, G6, G8).

-- One row per user. Phase 1 fills it from history (source = 'derived');
-- Phase 4's generator writes KYC-style profiles (source = 'generator'), which
-- refresh_account_profiles() never overwrites.
CREATE TABLE IF NOT EXISTS account_profiles (
    user_id            VARCHAR(32) PRIMARY KEY,
    home_city          VARCHAR(128),
    account_opened_at  TIMESTAMPTZ NOT NULL,
    avg_monthly_spend  NUMERIC(12, 2),
    source             TEXT NOT NULL CHECK (source IN ('derived', 'generator')),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Empty until Phase 4 seeds it from the merchant catalog. A missing merchant
-- means "unknown", not "risky".
CREATE TABLE IF NOT EXISTS merchant_risk (
    merchant    VARCHAR(255) PRIMARY KEY,
    risk_level  TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    reason      TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_ts ON transactions (user_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_merchant_ts ON transactions (merchant, event_timestamp);

-- The agent's only view of transactions: no is_fraud (ground truth), no card
-- digits, no ingest bookkeeping, and free text capped so a hostile merchant
-- name can't flood the model's context.
CREATE OR REPLACE VIEW v_agent_transactions AS
SELECT
    transaction_id,
    event_timestamp,
    user_id,
    amount,
    currency,
    left(merchant, 100)      AS merchant,
    merchant_category,
    card_type,
    left(location_city, 100) AS location_city,
    location_country
FROM transactions;

-- Upserts a derived profile for every user with transactions:
--   home_city          most frequent city (ties: most recently seen, then name)
--   account_opened_at  first transaction seen
--   avg_monthly_spend  total spend / months of history (at least 1)
-- Returns the number of rows written.
CREATE OR REPLACE FUNCTION refresh_account_profiles() RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    n integer;
BEGIN
    WITH city_counts AS (
        SELECT user_id, location_city, count(*) AS cnt, max(event_timestamp) AS last_seen
        FROM transactions
        WHERE location_city IS NOT NULL
        GROUP BY user_id, location_city
    ), home AS (
        SELECT DISTINCT ON (user_id) user_id, location_city
        FROM city_counts
        ORDER BY user_id, cnt DESC, last_seen DESC, location_city
    ), totals AS (
        SELECT user_id,
               min(event_timestamp) AS first_seen,
               sum(amount) / greatest(1, extract(epoch FROM max(event_timestamp) - min(event_timestamp)) / (30 * 86400))
                   AS monthly
        FROM transactions
        GROUP BY user_id
    ), upserted AS (
        INSERT INTO account_profiles (user_id, home_city, account_opened_at, avg_monthly_spend, source, updated_at)
        SELECT t.user_id, h.location_city, t.first_seen, round(t.monthly, 2), 'derived', now()
        FROM totals t
        LEFT JOIN home h USING (user_id)
        ON CONFLICT (user_id) DO UPDATE
            SET home_city = EXCLUDED.home_city,
                account_opened_at = EXCLUDED.account_opened_at,
                avg_monthly_spend = EXCLUDED.avg_monthly_spend,
                updated_at = now()
            WHERE account_profiles.source = 'derived'
        RETURNING 1
    )
    SELECT count(*) INTO n FROM upserted;
    RETURN n;
END
$$;

REVOKE ALL ON FUNCTION refresh_account_profiles() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION refresh_account_profiles() TO pipeline_writer;

GRANT SELECT, INSERT, UPDATE ON account_profiles, merchant_risk TO pipeline_writer;
GRANT SELECT ON account_profiles, merchant_risk TO worker;
GRANT SELECT ON v_agent_transactions, account_profiles, merchant_risk TO agent_ro;
