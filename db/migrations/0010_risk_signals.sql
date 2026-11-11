-- 0010 risk signals (docs/PLAN_HITL.md Phase H1).
--
-- Deterministic inputs for routing, computed after the fact so the Spark jobs stay
-- unchanged. The worker calls compute_incident_signals() right after it claims a row,
-- and stores the rule baseline's result next to the agent's as a second opinion.

ALTER TABLE investigations
    ADD COLUMN IF NOT EXISTS rule_hits        TEXT[],
    ADD COLUMN IF NOT EXISTS signals          JSONB,
    ADD COLUMN IF NOT EXISTS baseline_score   NUMERIC(5, 2),
    ADD COLUMN IF NOT EXISTS baseline_verdict TEXT,
    ADD COLUMN IF NOT EXISTS baseline_action  TEXT;

-- Fills rule_hits and signals for one incident. Write-once: a row that already has
-- them is left alone, so the values are the ones seen at first claim even if flags,
-- profiles or merchant data change later. Returns true if it wrote the row.
--
-- rule_hits mirror the thresholds in spark_fraud_detection.py (txn_count > 2,
-- total_amount > 300 in a 2-minute window), read from the incident's anomaly_flags.
-- signals:
--   max_txn_count, max_window_total   largest window among the incident's flags
--   primary_amount                    the primary transaction's amount
--   amount_to_avg_monthly_spend       primary_amount / account_profiles.avg_monthly_spend
--   primary_city_is_home              primary city = profile home_city
--   new_merchant_for_account          no transaction at that merchant before the incident
--   merchant_risk_level               merchant_risk.risk_level, 'unknown' if not listed
--   account_age_days                  incident_end date - account_opened_at date
-- A value that can't be known (no profile, no primary transaction) is null, not a guess.
CREATE OR REPLACE FUNCTION compute_incident_signals(p_id bigint) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    n integer;
BEGIN
    WITH inv AS (
        SELECT id, user_id, incident_start, incident_end, flag_ids, primary_txn_id
        FROM investigations
        WHERE id = p_id AND rule_hits IS NULL
    ), f AS (
        SELECT max(af.txn_count) AS max_count, max(af.total_amount) AS max_total
        FROM inv JOIN anomaly_flags af ON af.id = ANY(inv.flag_ids)
    ), p AS (
        SELECT t.amount, t.merchant, t.location_city
        FROM inv JOIN transactions t ON t.transaction_id = inv.primary_txn_id
    ), prof AS (
        SELECT ap.home_city, ap.avg_monthly_spend, ap.account_opened_at
        FROM inv JOIN account_profiles ap ON ap.user_id = inv.user_id
    ), computed AS (
        SELECT
            ARRAY(SELECT hit FROM (VALUES
                      ('spend_gt_300_in_2min', (SELECT max_total FROM f) > 300),
                      ('velocity_gt_2_in_2min', (SELECT max_count FROM f) > 2)
                  ) AS r(hit, fired)
                  WHERE fired ORDER BY hit) AS rule_hits,
            jsonb_build_object(
                'max_txn_count', (SELECT max_count FROM f),
                'max_window_total', (SELECT max_total FROM f),
                'primary_amount', (SELECT amount FROM p),
                'amount_to_avg_monthly_spend',
                    (SELECT round(p.amount / nullif(prof.avg_monthly_spend, 0), 2) FROM p, prof),
                'primary_city_is_home',
                    (SELECT p.location_city = prof.home_city FROM p, prof),
                'new_merchant_for_account',
                    (SELECT NOT EXISTS (
                         SELECT 1 FROM transactions t
                         WHERE t.user_id = inv.user_id AND t.merchant = p.merchant
                           AND t.event_timestamp < inv.incident_start)
                     FROM inv, p),
                'merchant_risk_level',
                    coalesce((SELECT mr.risk_level FROM p JOIN merchant_risk mr ON mr.merchant = p.merchant),
                             'unknown'),
                'account_age_days',
                    (SELECT (inv.incident_end AT TIME ZONE 'UTC')::date
                            - (prof.account_opened_at AT TIME ZONE 'UTC')::date
                     FROM inv, prof)
            ) AS signals
    )
    UPDATE investigations i
    SET rule_hits = c.rule_hits, signals = c.signals
    FROM computed c, inv
    WHERE i.id = inv.id;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n > 0;
END
$$;

REVOKE ALL ON FUNCTION compute_incident_signals(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION compute_incident_signals(bigint) TO worker;
