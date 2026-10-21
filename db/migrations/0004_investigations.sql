-- 0004 investigation queue (PLAN.md D1, D2).
--
-- One row per incident: a user's overlapping or touching anomaly_flags windows
-- merged into one span. Spark emits ~4 overlapping windows per event and
-- upserts them as counts grow, so flags are never enqueued one by one.

CREATE TABLE IF NOT EXISTS investigations (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             VARCHAR(32) NOT NULL,
    incident_start      TIMESTAMPTZ NOT NULL,
    incident_end        TIMESTAMPTZ NOT NULL,
    flag_ids            INTEGER[] NOT NULL,
    txn_ids             VARCHAR(64)[] NOT NULL,
    primary_txn_id      VARCHAR(64),             -- highest amount, latest on ties; NULL if no transactions
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'done', 'failed', 'dead')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    claimed_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    provider            TEXT,
    model               TEXT,
    prompt_version      TEXT,
    risk_score          NUMERIC(5, 2),
    verdict             TEXT,
    recommended_action  TEXT,
    explanation         TEXT,
    evidence            JSONB,
    trace               JSONB,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, incident_start)
);

CREATE INDEX IF NOT EXISTS idx_investigations_status_created ON investigations (status, created_at);

-- Merges settled flags into incidents and enqueues the new ones.
--   p_settle  an incident is enqueued only once incident_end + p_settle has passed
--             (window + watermark + trigger lag; also gives the separate raw-ingest
--             job time to write the transactions)
--   p_grace   an incident still without transactions after incident_end + p_grace
--             is recorded as 'failed' rather than dropped
--   p_since   flags with window_start before this are ignored
-- Idempotent: ON CONFLICT (user_id, incident_start) DO NOTHING. A flag that
-- lands after its incident was enqueued merges into the same incident_start
-- and so creates nothing. Returns the number of rows inserted.
CREATE OR REPLACE FUNCTION enqueue_incidents(
    p_settle interval    DEFAULT interval '5 minutes',
    p_grace  interval    DEFAULT interval '30 minutes',
    p_since  timestamptz DEFAULT '-infinity'
) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    n integer;
BEGIN
    WITH flags AS (
        SELECT id, user_id, window_start, window_end,
               max(window_end) OVER (
                   PARTITION BY user_id ORDER BY window_start, window_end, id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
               ) AS prev_end
        FROM anomaly_flags
        WHERE window_start >= p_since
    ), grouped AS (
        SELECT *,
               sum(CASE WHEN prev_end IS NULL OR window_start > prev_end THEN 1 ELSE 0 END) OVER (
                   PARTITION BY user_id ORDER BY window_start, window_end, id
               ) AS grp
        FROM flags
    ), incidents AS (
        SELECT user_id, min(window_start) AS incident_start, max(window_end) AS incident_end,
               array_agg(id ORDER BY id) AS flag_ids
        FROM grouped
        GROUP BY user_id, grp
    ), ready AS (
        SELECT i.*, tx.txn_ids, tx.primary_txn_id
        FROM incidents i
        CROSS JOIN LATERAL (
            SELECT array_agg(t.transaction_id ORDER BY t.event_timestamp, t.transaction_id) AS txn_ids,
                   (array_agg(t.transaction_id
                              ORDER BY t.amount DESC, t.event_timestamp DESC, t.transaction_id DESC))[1]
                       AS primary_txn_id
            FROM transactions t
            WHERE t.user_id = i.user_id
              AND t.event_timestamp >= i.incident_start
              AND t.event_timestamp <  i.incident_end
        ) tx
        WHERE i.incident_end + p_settle <= now()
          AND (tx.txn_ids IS NOT NULL OR i.incident_end + p_grace <= now())
    ), inserted AS (
        INSERT INTO investigations
            (user_id, incident_start, incident_end, flag_ids, txn_ids, primary_txn_id, status, error, finished_at)
        SELECT user_id, incident_start, incident_end, flag_ids, coalesce(txn_ids, '{}'), primary_txn_id,
               CASE WHEN txn_ids IS NULL THEN 'failed' ELSE 'pending' END,
               CASE WHEN txn_ids IS NULL THEN 'no transactions found in incident window' END,
               CASE WHEN txn_ids IS NULL THEN now() END
        FROM ready
        ON CONFLICT (user_id, incident_start) DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO n FROM inserted;
    RETURN n;
END
$$;

REVOKE ALL ON FUNCTION enqueue_incidents(interval, interval, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION enqueue_incidents(interval, interval, timestamptz) TO worker;

GRANT SELECT, INSERT, UPDATE ON investigations TO worker;
GRANT USAGE ON SEQUENCE investigations_id_seq TO worker;
