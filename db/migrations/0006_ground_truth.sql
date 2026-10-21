-- 0006 ground truth for synthetic data (planted patterns, injected anomalies).
-- Used only for evaluation: no grant to worker or agent_ro.

CREATE TABLE IF NOT EXISTS ground_truth_labels (
    id              BIGSERIAL PRIMARY KEY,
    label_type      TEXT NOT NULL,               -- e.g. 'ring', 'card_testing', 'decoy', 'impossible_travel'
    pattern_id      TEXT,                        -- groups the rows of one planted pattern
    user_id         VARCHAR(32),
    transaction_id  VARCHAR(64),
    details         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ground_truth_pattern ON ground_truth_labels (pattern_id);
CREATE INDEX IF NOT EXISTS idx_ground_truth_user ON ground_truth_labels (user_id);

GRANT SELECT, INSERT ON ground_truth_labels TO pipeline_writer;
GRANT USAGE ON SEQUENCE ground_truth_labels_id_seq TO pipeline_writer;
