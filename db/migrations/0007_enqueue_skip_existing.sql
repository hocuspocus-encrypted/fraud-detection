-- 0007 enqueue_incidents(): skip incidents that are already queued before inserting.
--
-- In 0004, every poll re-offered every existing incident to
-- INSERT ... ON CONFLICT DO NOTHING. Postgres draws a sequence value for each
-- attempted row even when it then conflicts, so investigations_id_seq advanced
-- by the size of the table on every tick (~146 ids per 30 s on fraud_db), and
-- each poll did conflict work proportional to the whole queue.
-- Same behavior otherwise; ON CONFLICT stays as the guard against two
-- concurrent enqueuers.

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
    ), new_incidents AS (
        SELECT i.*
        FROM incidents i
        WHERE i.incident_end + p_settle <= now()
          AND NOT EXISTS (
              SELECT 1 FROM investigations v
              WHERE v.user_id = i.user_id AND v.incident_start = i.incident_start
          )
    ), ready AS (
        SELECT i.*, tx.txn_ids, tx.primary_txn_id
        FROM new_incidents i
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
        WHERE tx.txn_ids IS NOT NULL OR i.incident_end + p_grace <= now()
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

-- CREATE OR REPLACE keeps existing grants; restated so this file stands alone.
REVOKE ALL ON FUNCTION enqueue_incidents(interval, interval, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION enqueue_incidents(interval, interval, timestamptz) TO worker;
