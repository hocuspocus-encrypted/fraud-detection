-- 0008 investigation worker bookkeeping (PLAN.md Phase 3).
--
-- next_attempt_at: a transient failure (rate limit, 5xx, timeout) puts the row
--   back to 'pending' with a backoff time instead of the worker sleeping on it,
--   so retries survive a worker restart.
-- investigation_attempts: one row per claim. Gives the hourly budget a durable
--   count across restarts, and shows what happened on each try.

ALTER TABLE investigations ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS investigation_attempts (
    id                BIGSERIAL PRIMARY KEY,
    investigation_id  BIGINT NOT NULL REFERENCES investigations (id) ON DELETE CASCADE,
    attempt           INTEGER NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    outcome           TEXT CHECK (outcome IN ('done', 'transient_error', 'timeout', 'failed', 'abandoned')),
    error             TEXT,
    UNIQUE (investigation_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_investigation_attempts_started ON investigation_attempts (started_at);

GRANT SELECT, INSERT, UPDATE ON investigation_attempts TO worker;
GRANT USAGE ON SEQUENCE investigation_attempts_id_seq TO worker;
