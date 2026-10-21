-- 0009 token usage per investigation, summed over all model turns, as the
-- provider adapter reports it (e.g. total_input_tokens, total_output_tokens,
-- total_tokens, turns). NULL for the rule-based baseline. Used to set
-- WORKER_HOURLY_BUDGET from measured cost (PLAN.md, risks: Cost).

ALTER TABLE investigations ADD COLUMN IF NOT EXISTS usage JSONB;
