-- 0013 analyst decisions and audit trail (docs/PLAN_HITL.md Phase H4).
--
--   analyst              role for the review UI (H5). Reads cases, never ground truth or the
--                        base transactions table. Created NOLOGIN; db/migrate.py gives it
--                        LOGIN when ANALYST_PASSWORD is set in .env. All analysts share it;
--                        the person's name comes from the app's login and is passed in.
--   analyst_decisions    append-only: no UPDATE or DELETE grant, and a trigger refuses both
--                        for every role. A later decision is a new row; the latest wins.
--   record_decision()    the only write path, and the only thing that sets
--                        investigations.review_status = 'reviewed'. Inserts the decision and
--                        updates the status in one transaction. analyst has no direct INSERT
--                        on the table, so a decision can't skip the status rules below.
--
-- agree / override close the case (reviewed). request_info is recorded and the case stays
-- in the queue. Cases that can be decided: needs_review, held_pending_review, an
-- auto_cleared QA sample, and reviewed ones (a second decision on a closed case).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE ROLE analyst NOLOGIN;
    END IF;
END
$$;

ALTER ROLE analyst SET statement_timeout = '30s';
GRANT USAGE ON SCHEMA public TO analyst;

CREATE TABLE IF NOT EXISTS analyst_decisions (
    id                         BIGSERIAL PRIMARY KEY,
    investigation_id           BIGINT NOT NULL REFERENCES investigations (id),
    analyst                    TEXT NOT NULL CHECK (btrim(analyst) <> '' AND length(analyst) <= 100),
    decision                   TEXT NOT NULL CHECK (decision IN ('agree', 'override', 'request_info')),
    final_action               TEXT CHECK (final_action IN ('approve', 'hold', 'block', 'request_otp')),
    reason                     TEXT CHECK (length(reason) <= 2000),
    agent_verdict_at_decision  TEXT,              -- investigations.verdict when the decision was made
    decided_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT override_needs_reason CHECK (decision <> 'override' OR btrim(coalesce(reason, '')) <> ''),
    CONSTRAINT final_action_required CHECK (final_action IS NOT NULL OR decision = 'request_info')
);

CREATE INDEX IF NOT EXISTS idx_analyst_decisions_case
    ON analyst_decisions (investigation_id, decided_at DESC, id DESC);

CREATE OR REPLACE FUNCTION analyst_decisions_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'analyst_decisions is append-only; record a new decision instead';
END
$$;

DROP TRIGGER IF EXISTS analyst_decisions_append_only ON analyst_decisions;
CREATE TRIGGER analyst_decisions_append_only
    BEFORE UPDATE OR DELETE ON analyst_decisions
    FOR EACH ROW EXECUTE FUNCTION analyst_decisions_append_only();

CREATE OR REPLACE FUNCTION record_decision(
    p_investigation_id bigint,
    p_analyst          text,
    p_decision         text,
    p_final_action     text,
    p_reason           text DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    inv    record;
    new_id bigint;
BEGIN
    SELECT id, review_status, qa_sample, verdict INTO inv
    FROM investigations WHERE id = p_investigation_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'investigation % does not exist', p_investigation_id
            USING ERRCODE = 'no_data_found';
    END IF;
    -- coalesce: an unrouted row (NULL status) would otherwise make the test NULL and pass.
    IF NOT coalesce(inv.review_status IN ('needs_review', 'held_pending_review', 'reviewed')
                    OR (inv.review_status = 'auto_cleared' AND inv.qa_sample), false) THEN
        RAISE EXCEPTION 'investigation % is not open for review (review_status %)',
            p_investigation_id, coalesce(inv.review_status, 'not routed')
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

    -- The table's CHECKs reject an override without a reason and unknown values.
    INSERT INTO analyst_decisions (investigation_id, analyst, decision, final_action, reason,
                                   agent_verdict_at_decision)
    VALUES (p_investigation_id, btrim(p_analyst), p_decision, p_final_action,
            nullif(btrim(p_reason), ''), inv.verdict)
    RETURNING id INTO new_id;

    IF p_decision <> 'request_info' THEN
        UPDATE investigations SET review_status = 'reviewed' WHERE id = p_investigation_id;
    END IF;
    RETURN new_id;
END
$$;

REVOKE ALL ON FUNCTION record_decision(bigint, text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION record_decision(bigint, text, text, text, text) TO analyst;

-- One row per decided case: its latest decision, and how many it has had.
CREATE OR REPLACE VIEW v_case_latest_decision AS
SELECT DISTINCT ON (investigation_id)
    investigation_id,
    id AS decision_id,
    analyst,
    decision,
    final_action,
    reason,
    agent_verdict_at_decision,
    decided_at,
    count(*) OVER (PARTITION BY investigation_id) AS decision_count
FROM analyst_decisions
ORDER BY investigation_id, decided_at DESC, id DESC;

GRANT SELECT ON investigations, investigation_attempts, v_agent_transactions,
                analyst_decisions, v_case_latest_decision TO analyst;
