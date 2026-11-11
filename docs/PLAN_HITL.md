# Plan: Human-in-the-loop review, risk signals, and finishing the agent pipeline

**Audience:** Claude Code, working in the `fraud-detection` repo. Read `CLAUDE.md`, `PLAN.md` and `docs/CURRENT_STATE.md` first. This file adds phases to `PLAN.md`; it does not replace it. All of `PLAN.md` section 2 (ground rules) still applies.

**Written:** 2026-11-11, from a read of commit `742e83a` ("Phase 1-4 completed"). Phases 0-4 of `PLAN.md` are built. Without Postgres, 23 tests pass; the 2 that need a database were not run.

**Corrected 2026-11-11 against commit `fb92d65`.** `PLAN.md` Phase 5 is also done (`docs/METRICS.md`). The agent now runs on `qwen3:4b-instruct`, and the worker stores the verdict and action from the band of the model's `risk_score`, keeping the model's own label in the trace (`verdict_from_score` in `investigation_worker.py`). `v3` already names a prefetch prompt that was tried and rejected, so this plan's new prompt contract is `v4`. The phases below are otherwise as written.

---

## 1. Why this plan exists

The design shared with a reviewer describes a human-in-the-loop (HITL) layer that the code does not have yet. What exists and what is missing:

| Workflow step | Status in code | Gap |
|---|---|---|
| 1. Flag the transaction | Built. `spark_fraud_detection.py`, 2-min sliding window, `txn_count > 2 OR total_amount > 300` | Output is a boolean flag only: no rule names, no score, no model. The rules rarely catch impossible travel (`docs/CURRENT_STATE.md` section 10). |
| 2. Pick up the flag | Built. `investigation_enqueuer.py` + `enqueue_incidents()` merge windows into incidents, settle delay, idempotent | None |
| 3. Gather context | Built. 4 read-only tools on `agent_ro`, `v_agent_transactions` hides `is_fraud`, output caps | No cross-account tools yet (`PLAN.md` Phase 6) |
| 4. Reason and recommend | Built. JSON verdict with `risk_score`, `verdict`, `recommended_action`, `evidence_cited` | No `confidence` field. Evidence is free text and not checked against the tool trace. Agent (`qwen3:4b-instruct`, verdict from score) on the incident eval test split: 80% strict, all fraud caught, but 18% of good customers would be blocked (`docs/METRICS.md`). |
| 5. Route the case | **Missing** | `recommended_action` is stored and nothing acts on it. None of the escalation triggers exist. |
| 6. Analyst decides | **Missing** | The dashboard does not read `investigations` at all. No decision table, no override reasons, no analyst role. |
| 7. Log and learn | Partly built. Trace, attempts, usage, prompt version stored | No human-decision audit, no override metrics, no route from overrides back into the eval set |
| Oversight controls | Partly built. Kill switch (`WORKER_ENABLED`), hourly budget, timeouts | No rules-only fallback mode |

The order below closes steps 5-7 first, because that is the design already described to the reviewer, then returns to `PLAN.md` Phase 9.

## 2. Decisions to confirm with Vib before starting

Ask these at the start of Phase H0. Use the default if he says "go with your recommendation".

**Answered 2026-11-11:** the defaults, with Q4 changed to `qwen3:4b-instruct` because `qwen3:8b` doesn't fit in this laptop's memory.

| # | Question | Default |
|---|---|---|
| Q1 | Review amount limit. The data is in **USD**, and the reviewer was told "e.g. AED 10,000". | `REVIEW_AMOUNT_LIMIT_USD=2500` in `.env`, documented as roughly AED 9,200 |
| Q2 | Uncertain score band | 30 to 70 inclusive, in `.env` |
| Q3 | QA sample rate for auto-cleared cases | 10%, deterministic from the investigation id |
| Q4 | Which models to compare in Phase H8 ~~qwen3:8b~~ `qwen3:4b-instruct` (Ollama) and Gemini; Claude only if Vib has a key |
| Q5 | Merge the standalone `fraud-agent` repo into this one | Keep it as the scaffold and point its README here (H0) |

## 3. Phases

One phase per session. Write the acceptance checks as tests first, then implement, run the full test suite, then commit. Migrations continue from `0010`.

### Phase H0: Housekeeping (small)

- Add `CLAUDE.md` at the repo root (a draft comes with this plan).
- `git rm -r --cached checkpoints` (Spark checkpoint files are committed even though `checkpoints/` is in `.gitignore`).
- In the `fraud-agent` repo: rename `README (1).md` to `README.md` (GitHub won't render it as the front page with the current name) and add one line at the top saying the integrated version lives in `fraud-detection/agent/`.
- Do not change the hardcoded `fraud_user`/`fraud_pass` in the Spark jobs (ground rule: leave the fragile pipeline alone). New code reads credentials from `.env`.

**Acceptance:** `git ls-files checkpoints` prints nothing; the test suite still passes.

### Phase H1: Risk signals on every incident (step 1, without touching Spark)

The routing in H3 needs real inputs, and the Spark jobs must stay unchanged. So compute deterministic signals after the fact, in Postgres or the worker.

1. Migration `0010_risk_signals.sql`: add to `investigations`
   - `rule_hits TEXT[]`: which pipeline rules fired across the incident's flags (`velocity_gt_2_in_2min`, `spend_gt_300_in_2min`), derived from `anomaly_flags` rows in `flag_ids`
   - `signals JSONB`: deterministic features for the incident: `max_txn_count`, `max_window_total`, `primary_amount`, `amount_to_avg_monthly_spend`, `primary_city_is_home`, `new_merchant_for_account`, `merchant_risk_level`, `account_age_days`
   - `baseline_score NUMERIC(5,2)`, `baseline_verdict TEXT`, `baseline_action TEXT`
2. Fill `rule_hits` and `signals` in a SQL function `compute_incident_signals(p_id bigint)`, called by the worker right after it claims a row (the worker role may read `transactions`; the agent may not). Keep it idempotent.
3. In the worker, run `adapters.baseline_adapter.run_investigation` on the same incident before the LLM and store its result in the `baseline_*` columns. It is free, takes milliseconds and uses the same read-only tools. This is the second opinion that the "rules and model disagree" trigger compares against. Be honest in docs: it is a rule scorer, not an ML model.
4. Pass `rule_hits` into the agent's `context` string so the agent knows *why* the case was flagged. Bump `PROMPT_VERSION` to `v4` if the task text changes.

**Acceptance:** tests on a throwaway DB: a velocity-only burst gets `rule_hits = {velocity_gt_2_in_2min}`; a single $400 purchase gets `{spend_gt_300_in_2min}`; re-running the function on a row does not change it; the baseline columns are filled for every `done` row; `agent_ro` still cannot read `investigations`.

### Phase H2: Verdict contract v4 (step 4)

1. Update `SYSTEM_PROMPT` in `agent/adapters/shared.py` so the JSON adds:
   - `"confidence": "<low|medium|high>"`
   - `"evidence_cited"` items become objects `{"claim": "...", "tool": "<tool name>"}` (keep accepting plain strings from older rows when reading)
2. Update `valid_verdict` in `investigation_worker.py` for the new fields. A missing `confidence` is treated as `low`, not as a failure.
3. Add a grounding check, `grounding_ok`: every `tool` named in `evidence_cited` must appear as a `tool_call` in the trace. Store the result in a new `grounding_ok BOOLEAN` column. Do not fail the investigation on it; routing uses it in H3.
4. Bump `PROMPT_VERSION` to `v4` (add it to `PROMPT_VERSIONS`).
5. Re-run `agent/eval.py` for the baseline and the Ollama model on the 15 cases. Record strict and lenient scores for v2 and v4 in `docs/METRICS.md`. If v4 is worse, report it. Do not tune the prompt to recover the number in this phase.

**Acceptance:** adapter tests cover the new fields and the old string form; the grounding check flags a verdict that cites a tool that was never called; eval numbers recorded.

### Phase H3: Deterministic routing (step 5)

New module `routing.py` at the repo root, pure functions only, no database or LLM calls. The worker calls it inside `_finish` and writes the result in the same transaction.

Migration `0011_review.sql`: add to `investigations`

- `review_status TEXT` with CHECK in (`auto_cleared`, `needs_review`, `held_pending_review`, `reviewed`), NULL until routed
- `review_reasons TEXT[]`
- `review_priority INTEGER` (higher = sooner)
- `qa_sample BOOLEAN NOT NULL DEFAULT false`

`route(verdict, signals, baseline, grounding_ok, status, config) -> (review_status, reasons, priority, qa_sample)`. The triggers, each a separate reason code:

| Reason code | Fires when |
|---|---|
| `uncertain_score` | `risk_score` is in the configured band (Q2) |
| `agent_baseline_disagree` | the agent's verdict and the baseline verdict differ, comparing `legitimate` against anything else |
| `amount_over_limit` | `primary_amount > REVIEW_AMOUNT_LIMIT_USD` (Q1) |
| `irreversible_action` | `recommended_action = block_and_notify_customer` (always reviewed; nothing is ever blocked automatically) |
| `low_confidence` | `confidence = low` |
| `ungrounded_evidence` | `grounding_ok = false` |
| `agent_failed` | investigation `status` is `failed` or `dead` (the router also runs for these) |
| `new_account` | `account_age_days < 30` (proxy for "pattern looks new"; document it as a proxy) |

Outcomes:

- No reasons, verdict `legitimate`, baseline also `legitimate` → `auto_cleared`. Mark `qa_sample = true` for Q3's share, chosen deterministically (`id % 10 == 0` at 10%), and give those the lowest priority in the queue.
- Any reason plus a recommended action of `hold_for_review` or `block_and_notify_customer` → `held_pending_review`. The hold is a **recorded state only**. The system takes no action on any account.
- Any other reason → `needs_review`.
- Priority: `risk_score` + 20 per reason, + 50 for `irreversible_action`.

All thresholds come from `.env` through a small `RoutingConfig` dataclass, like `WorkerConfig`.

**Acceptance:** one unit test per trigger and per outcome; a table-driven test that a clear legitimate case auto-clears and that a `block_and_notify_customer` recommendation can never auto-clear, whatever the score; a backfill script `route_existing.py` routes old `done`/`failed`/`dead` rows and is safe to re-run.

### Phase H4: Analyst decisions and audit trail (steps 6-7, database side)

Migration `0012_analyst.sql`:

- Role `analyst` (password from `.env`, NOLOGIN if unset). It can SELECT `investigations`, `investigation_attempts` and `v_agent_transactions`, and SELECT/INSERT on `analyst_decisions`. It gets no UPDATE or DELETE on `analyst_decisions` (append-only) and no access to `ground_truth_labels` or the base `transactions` table.
- Table `analyst_decisions`: `id`, `investigation_id` (FK), `analyst` (text, from the app's login name), `decision` (`agree`, `override`, `request_info`), `final_action` (`approve`, `hold`, `block`, `request_otp`), `reason` (required when `decision = override`, enforced by CHECK), `agent_verdict_at_decision`, `decided_at`.
- Function `record_decision(...)`, `SECURITY DEFINER`, the only path that sets `investigations.review_status = 'reviewed'`. It inserts the decision and updates the status in one transaction, and rejects decisions on rows that are not `needs_review`, `held_pending_review` or a QA sample.

**Acceptance:** an override without a reason is rejected; `analyst` cannot UPDATE or DELETE a decision; `agent_ro` cannot read `analyst_decisions`; two decisions on one case keep both rows and the latest wins in a view `v_case_latest_decision`.

### Phase H5: Streamlit review queue (step 6, UI; replaces part of `PLAN.md` Phase 8)

Turn the dashboard into a multipage app (`pages/`) and keep the current page as "Pipeline". New pages connect as `analyst` through `db.connect_kwargs`, never as `fraud_user`.

1. **Review queue:** `needs_review` and `held_pending_review` cases, plus open QA samples, sorted by `review_priority`. Columns: user, amount, agent verdict and score, baseline verdict, reasons, age of the case.
2. **Case detail:** incident transactions, rule hits, signals, agent explanation and evidence (with the tool each cites), a collapsible tool trace, baseline result, and previous decisions. A decision form: agree / override / request info, final action, reason (required for override, checked before submit). Submits through `record_decision`.
3. **Oversight:** counts by `review_status`, queue depth, median time to decision, override rate, the H6 metrics.

**Acceptance:** a written walkthrough checklist in `docs/REVIEW_UI_CHECKLIST.md`, completed: open a held case, override it with a reason, confirm the decision row exists and the case leaves the queue; submitting an override without a reason shows an inline error and writes nothing.

### Phase H6: Feedback loop and oversight metrics (step 7)

1. View `v_oversight_metrics` over a time range: agreement rate (analyst `agree`), override rate split by direction (agent said fraud → analyst approved, and the reverse), share of cases routed to review, share auto-cleared, QA-sample override rate (how often auto-clearing was wrong), median decision time.
2. Ground-truth check, synthetic data only, admin role, for the evaluation page and docs only: precision and recall of the agent, the baseline and the analyst-final decision against `ground_truth_labels`. Clearly labelled as synthetic.
3. Script `export_reviewed_cases.py`: writes overridden cases as candidate eval cases (same shape as `agent/eval_cases.json`) into `agent/eval_candidates.json` for Vib to review by hand. It never edits `eval_cases.json` directly.

**Acceptance:** metric view tested on seeded decisions with known answers; export script produces valid case objects.

### Phase H7: Rules-only fallback

- `.env` setting `AGENT_MODE=agent|rules_only`, re-read each loop like `WORKER_ENABLED`.
- In `rules_only`, the worker skips the LLM, stores the baseline result as the verdict (`provider = baseline`), and routing treats every non-legitimate case as `needs_review` (add reason `rules_only_mode`).
- Log a `mode_changed` event when it flips.

**Acceptance:** flipping the setting mid-run changes the next claim without a restart; no LLM call is made in `rules_only` (test with a fake investigate function that fails if called).

### Phase H8: Agent quality (step 4, after HITL is in place)

The local agent catches all fraud on the incident eval but would block 18% of good customers, mostly legitimate travel and big purchases scored above 70 (`docs/METRICS.md`). Diagnose before changing anything.

1. Write `docs/AGENT_MISSES.md`: for each case the agent gets wrong, the trace, which tool output it misread or skipped, and the likely cause (prompt, tool gap, model size).
2. Only then try fixes, one at a time, re-running the eval each time: passing `rule_hits` and `signals` in the context (from H1); a stronger model (Q4); a narrow new tool only if the miss log shows a real gap.
3. `PLAN.md` Phase 9 item: run each provider 3 times and report the mean and the verdict flip rate.

**Acceptance:** `docs/METRICS.md` has a table of provider × prompt version × run with strict/lenient scores and flips. No threshold or case is changed to improve a score.

### Then: `PLAN.md` Phase 5 additions, Phase 9, and README

- **Phase 5 (end to end, already done once):** rerun it and also record review-queue depth, the share of cases routed to review, and flag-to-decision latency.
- **Phase 9 + README:** the README still ends at "Day 4". Rewrite it with the full architecture (pipeline → enqueuer → worker → routing → review queue), how to run it on Windows, the metrics table, and a plain Limitations section (synthetic data, rule scorer rather than ML model, local model accuracy, `new_account` as a proxy for novelty, holds are simulated).
- **Phases 6-7 (similarity, cluster summaries):** after the above, as planned.

### Optional: Phase R, an impossible-travel detector

The Spark rules almost never catch impossible travel, so the agent never sees those cases. If Vib wants it fixed, do it **without changing the Spark jobs**: a SQL function, run by the enqueuer every loop, that finds consecutive transactions per user in different countries less than N hours apart and writes them to a new `rule_flags` table, which `enqueue_incidents` then merges like `anomaly_flags`. Measure recall on planted travel fraud before and after. Ask before starting this one.

## 4. Order and rough size

| Order | Phase | Size |
|---|---|---|
| 1 | H0 housekeeping | small |
| 2 | H1 risk signals + baseline second opinion | medium |
| 3 | H2 verdict contract v4 | small-medium |
| 4 | H3 routing | medium |
| 5 | H4 analyst decisions | medium |
| 6 | H5 review UI | medium-large |
| 7 | H6 metrics + feedback | medium |
| 8 | H7 rules-only fallback | small |
| 9 | H8 agent quality | medium |
| 10 | PLAN.md Phase 5 rerun, Phase 9, README | medium |
| later | PLAN.md Phases 6-7, optional R | large |

H1-H4 give a demonstrable HITL loop from the command line. H5 makes it visible.

## 5. Kickoff prompt for each session

```
Read CLAUDE.md, PLAN.md, docs/CURRENT_STATE.md and docs/PLAN_HITL.md.
We are doing Phase <Hn> of docs/PLAN_HITL.md only.
Start by listing the acceptance checks for this phase and writing them as tests.
Ask me before changing anything under docker-compose.yml, the Spark jobs, or environment setup.
When the checks pass, update the progress line at the top of PLAN.md and commit.
```
