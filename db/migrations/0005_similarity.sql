-- 0005 cross-account similarity output (PLAN.md Phase 6). Written by the batch
-- job, read by the agent tools. Empty until Phase 6.

CREATE TABLE IF NOT EXISTS account_similarity (
    account_a     VARCHAR(32) NOT NULL,
    account_b     VARCHAR(32) NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    score         NUMERIC(6, 4) NOT NULL,
    components    JSONB NOT NULL,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_a, account_b, window_start, window_end),
    CHECK (account_a < account_b)                -- each pair stored once
);

CREATE INDEX IF NOT EXISTS idx_account_similarity_b ON account_similarity (account_b, window_start);

CREATE TABLE IF NOT EXISTS fraud_clusters (
    cluster_id    BIGSERIAL PRIMARY KEY,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    size          INTEGER NOT NULL,
    reason        TEXT NOT NULL,                 -- built deterministically from shared evidence
    evidence      JSONB NOT NULL,
    summary       TEXT,                          -- Phase 7, optional LLM case summary
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fraud_cluster_members (
    cluster_id  BIGINT NOT NULL REFERENCES fraud_clusters (cluster_id) ON DELETE CASCADE,
    user_id     VARCHAR(32) NOT NULL,
    PRIMARY KEY (cluster_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_fraud_cluster_members_user ON fraud_cluster_members (user_id);

GRANT SELECT ON account_similarity, fraud_clusters, fraud_cluster_members TO agent_ro, worker;
