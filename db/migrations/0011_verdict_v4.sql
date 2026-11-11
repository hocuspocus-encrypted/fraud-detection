-- 0011 verdict contract v4 (docs/PLAN_HITL.md Phase H2).
--
-- confidence    the model's stated confidence; 'low' when it gave none or an unknown
--               value (also for v2 rows, whose prompt doesn't ask for it). NULL on rows
--               without a verdict.
-- grounding_ok  under v4, whether every tool named in evidence_cited was called without
--               an error in this investigation's trace. NULL when not checked (older
--               prompts name no tools). Routing (H3) reads it; it never fails a row.

ALTER TABLE investigations
    ADD COLUMN IF NOT EXISTS confidence   TEXT CHECK (confidence IN ('low', 'medium', 'high')),
    ADD COLUMN IF NOT EXISTS grounding_ok BOOLEAN;
