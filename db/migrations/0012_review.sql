-- 0012 deterministic routing (docs/PLAN_HITL.md Phase H3).
--
-- Written by the worker, in the same transaction as the investigation result, from
-- routing.route(); route_existing.py backfills rows finished before this migration.
-- All NULL (qa_sample false) until a row is routed.
--
-- review_status    auto_cleared | needs_review | held_pending_review | reviewed. A hold is a
--                  recorded state for an analyst; nothing acts on the account. 'reviewed'
--                  is set only by the analyst decision path (H4).
-- review_reasons   reason codes from routing.REASONS
-- review_priority  higher = sooner: risk_score + 20 per reason, + 50 for irreversible_action;
--                  0 for auto-cleared cases, so QA samples sort last
-- qa_sample        an auto-cleared case picked deterministically for QA review

ALTER TABLE investigations
    ADD COLUMN IF NOT EXISTS review_status   TEXT
        CHECK (review_status IN ('auto_cleared', 'needs_review', 'held_pending_review', 'reviewed')),
    ADD COLUMN IF NOT EXISTS review_reasons  TEXT[],
    ADD COLUMN IF NOT EXISTS review_priority INTEGER,
    ADD COLUMN IF NOT EXISTS qa_sample       BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_investigations_review_queue
    ON investigations (review_status, review_priority DESC);
