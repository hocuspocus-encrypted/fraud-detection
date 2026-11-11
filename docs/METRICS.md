# Phase 5 Metrics: End to End on the Live Stream

Run of 2026-10-25, 11:23 to 12:41 UTC (78 minutes). Numbers come from `phase5_metrics.py`, run against Postgres after the run. The script's own checks are in `tests/test_phase5_metrics.py`.

## Setup

| | |
|---|---|
| Hardware | Windows 11 laptop on AC power, RTX 4050 Laptop GPU (6 GB), 15 GB RAM. Kafka and Postgres in Docker; both Spark jobs, the enqueuer, the worker and Ollama on the host. |
| Stream | `kafka_producer.py --interval 0.5`: about 120 transactions/min, 5000 generated customers (seed 42) |
| Detection | Unchanged Spark rules: more than 2 transactions or more than $300 in a 2-minute window, 30 s slide |
| Queue | Enqueuer polls every 30 s; an incident is queued 5 min after its last window ends (PLAN.md D2) |
| Agent | `qwen3:8b` through Ollama, prompt `v2`, 1 investigation at a time, budget 200/hour, 180 s timeout |

Periods (UTC):

| Period | From | To |
|---|---|---|
| Worker on | 11:23:22 | 11:53:56 |
| Worker stopped (process killed) | 11:53:56 | 12:14:20 |
| Worker restarted | 12:14:20 | 12:41:30 (backlog drained by 12:31:08) |

`phase5_metrics.py` found no gap longer than 60 s in the transaction stream, so the machine stayed awake throughout. An earlier attempt (2026-10-22) was thrown away: the laptop was on battery and went to sleep 10 minutes in.

## Latency

Measured from the incident's **first transaction**, which makes it an upper bound on flag-to-verdict, because the anomaly job overwrites `anomaly_flags.detected_at` on every upsert. Only incidents that reached `done` are counted.

| Incidents queued | n | Total p50 | Total p95 | Settle p50 | Queue wait p50 | Queue wait p95 | Investigation p50 |
|---|---|---|---|---|---|---|---|
| While the worker was on | 20 | **7.8 min** | 28.2 min | 7.3 min | 3 s | 20.5 min | 23 s |
| While the worker was stopped | 19 | 24.2 min | 27.1 min | 7.3 min | 16.6 min | 19.1 min | 25 s |
| After the restart | 30 | 10.8 min | 16.4 min | 7.3 min | 2.8 min | 8.6 min | 24 s |
| **Whole run** | 69 | 11.4 min | 27.0 min | 7.3 min | 3.1 min | 18.8 min | 24 s |

- **Steady state is about 8 minutes, and most of it is deliberate.** The settle stage (first transaction to queued, about 7.3 min) is the 2-minute window, the incident's own span, the 5-minute settle delay and up to 30 s of polling. When the worker keeps up, queue wait is seconds and the investigation about 24 s. The p95 for "worker on" comes from incidents queued just before the worker was stopped.
- PLAN.md's original target (D5) was 1-2 minutes. D2 moved it to about 6-7 minutes to get one investigation per incident. The measured 7.8 min matches that.
- **Per investigation:** 2,331 tokens and 22 s of model time on average, 2.6 tool calls.

## Queue depth

Incidents queued and not yet finished, every 5 minutes:

| 11:23 | 11:28 | 11:33 | 11:38 | 11:43 | 11:48 | 11:53 | **11:58** | **12:03** | **12:08** | **12:13** | 12:18 | 12:23 | 12:28 | 12:33 | 12:38 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 2 | 0 | 0 | 0 | 0 | **6** | **12** | **17** | **21** | 17 | 12 | 5 | 1 | 1 |

About 1 incident a minute arrived (69 in 78 minutes, within the 60-75/hour simulated in Phase 4). With the worker stopped, the queue grew by about 1 a minute. After the restart, the 21-incident backlog drained in about 17 minutes while new incidents kept arriving.

## Failures

| | Count |
|---|---|
| Incidents done | 69 |
| Incidents failed or dead | 0 (failure rate 0%) |
| Attempts retried | 1: Ollama HTTP 500, `CUDA error: out of memory`. Classified as transient and retried 30 s later; the retry succeeded. |
| Left unfinished at the end | 2 (1 pending, 1 in progress when the run was stopped) |

The out-of-memory error fits the model not fitting in 6 GB of VRAM (Ollama runs 30-37% of `qwen3:8b` on the CPU at an 8192-token context).

## Decoupling: does the worker affect Spark?

Spark health per period, from 5-minute buckets:

| Period | Transactions ingested/min | Ingest lag p50 | Ingest lag p95 (range of buckets) | Flag rows/min |
|---|---|---|---|---|
| Worker on (mostly idle, 1 incident/min) | 119.3 | 3.0 s | 3.6-5.1 s | 3.3 |
| **Worker stopped** | 120.3 | 3.1 s | 4.6-7.6 s | 3.3 |
| Worker restarted (busy draining the backlog) | 112.6* | 5.1 s | 7.6-11.5 s | 3.3 |

\* The last bucket is cut off by the end of the run (425 transactions in 3.5 minutes); without it the rate is 119.8/min.

- **Stopping the worker had no effect on Spark.** Throughput, ingest lag and flag rate were the same with the worker on and off. The stream doesn't wait on the investigation queue.
- **A busy worker slows Spark a little, through shared hardware rather than the design.** While the worker ran investigations back to back (12:14-12:31), ingest lag p50 rose from about 3 s to about 5 s, and p95 from about 5 s to up to 11.5 s. No batches stalled and throughput held. Ollama, which runs a third of the model on the CPU, competes with Spark for the laptop's CPU. On separate machines this would go away. Here it is the realistic cost of running everything on one laptop.

## Agent verdicts vs ground truth (informational)

Done incidents only. This is not the Phase 9 evaluation; it's here because it's the first time live verdicts can be scored.

| Incident contains a labeled fraud transaction | legitimate | suspicious | likely_fraud |
|---|---|---|---|
| Yes (19) | 0 | 11 | 8 |
| No (50) | 0 | 43 | 7 |

**The agent never cleared an incident.** It approved no fraud, which is good, but it also called 43 of the 50 non-fraud incidents `suspicious` and blocked 7. So far it adds review work rather than filtering it. That fits what the 15-case eval showed for `qwen3:8b`: it skips tools (2.6 calls per investigation here, out of 4), and its scores and labels don't line up. Fixing that is prompt and model work (see PLAN.md), not part of Phase 5.

## Acceptance (PLAN.md Phase 5)

| Check | Result |
|---|---|
| Latency, queue depth and failure rate recorded | Done (above) |
| Spark keeps processing with the worker stopped | Yes: same throughput and ingest lag with the worker stopped |
| Backlog drains after restart | Yes: 21 incidents cleared in about 17 minutes |
| Spark unaffected by a slow worker | Partly. The stream never stalls, but a busy local model raises Spark's ingest lag from about 3 s to about 5 s (p95 up to 11.5 s) through CPU contention on the shared laptop. |

## After Phase 5: incident eval and the score-derived verdict (2026-10-29)

The live verdicts above came from `qwen3:8b` and no longer describe the agent. Since then:

- **Incident eval** (`agent/incident_eval.py`): labelled incidents built from the generator (dev: stream seed 1001, 99 cases; test: stream seed 2002, 171 cases), each run through the real worker with history only up to the incident end + 5 minutes. Fraud should be `likely_fraud` (`suspicious` is lenient); a legitimate oddity or an ordinary incident should be `legitimate` (`suspicious` is lenient for oddities).
- **Model: `qwen3:4b-instruct`.** `qwen3:8b` ran a third on the CPU and ran this 15 GB laptop out of RAM. The real cause turned out to be Ollama's llama-server prompt cache (8 GB limit, not configurable); the adapter now unloads the model every 8 requests (`OLLAMA_RECYCLE_AFTER_REQUESTS`).
- **Prompt v3** (all four checks prefetched, label must match the score band) was tried and rejected.
- **The verdict now follows the score.** The worker stores the band of the model's `risk_score` (0-30 legitimate, 31-70 suspicious, 71-100 likely_fraud) as the verdict and action, and keeps the model's own label in the trace (`verdict_from_score`). The bands were fixed before the dev runs and not tuned.

Dev split, 3 runs x 99 cases, `qwen3:4b-instruct`:

| | v2, model's label | v3, model's label | v2, label from score |
|---|---|---|---|
| Fraud not cleared | 100% | 100% | 98% |
| Fraud labelled likely_fraud | 90% | 98% | 96% |
| Legitimate cleared | 7% | 22% | 78% |
| Good customers blocked | 1% | 21% | 14% |
| Strict / lenient | 30% / 80% | 43% / 77% | 83% / 87% |
| Verdict flips between runs | 7% | 13% | |
| Seconds per case | 11.0 | 8.3 | 11.0 |

Test split, run once with the chosen configuration (v2, label from score), 171 cases:

| Fraud not cleared | Fraud likely_fraud | Legitimate cleared | Good customers blocked | Strict / lenient |
|---|---|---|---|---|
| 100% (59/59) | 95% | 72% | 18% | 80% / 88% |

Good customers blocked splits as 11% of ordinary incidents and 22% of legitimate oddities (travel, big purchases, new merchants). That comes from the model's scores, not the bands. Synthetic data; one test run.

## HITL H2: prompt v2 vs v4 (2026-11-11)

Prompt `v4` is `v2` plus the rules that fired in the incident context, a `confidence` field, and each `evidence_cited` item naming the tool it came from (docs/CURRENT_STATE.md section 12). Both prompts were run on the same day with the same code, on `qwen3:4b-instruct` with the label taken from the score, as the worker stores it. Since H2, the incident eval inserts each incident's flag windows, so `rule_hits` is filled (v4 sees them; v2 doesn't). The prompt was not tuned after these runs.

**Incident eval, dev split, 3 runs x 99 cases:**

| | v2 (2026-10-29) | v2 (today) | v4 (today) |
|---|---|---|---|
| Fraud not cleared | 98% | 98% | **100%** |
| Fraud labelled likely_fraud | 96% | 96% | **100%** |
| Legitimate cleared | 78% | **77%** | 69% |
| Good customers blocked | 14% | **12%** | 17% |
| Strict / lenient | 83% / 87% | **82% / 89%** | 78% / 88% |
| Verdict flips between runs | | **7%** | 10% |
| Seconds per case | 11.0 | 11.7 | 12.0 |
| All four checks run | 100% | 99% | 93% |
| Evidence grounded in the trace | | not checked | 99% (2 of 297 not) |
| Confidence given | | none (stored as low) | high 290, medium 7, low 0 |

Today's v2 run reproduces the 2026-10-29 one within a point or two, so the comparison isn't skewed by drift since then.

By category, all 3 runs together (legitimate / suspicious / likely_fraud):

| | v2 | v4 |
|---|---|---|
| Fraud (84) | 2 / 1 / 81 | 0 / 0 / 84 |
| Legitimate oddity (150) | 109 / 18 / 23 | 97 / 28 / 25 |
| Ordinary (63) | 54 / 6 / 3 | 51 / 0 / 12 |

**15-case eval (`agent/eval.py`):**

| | baseline (rules) | v2 | v4 |
|---|---|---|---|
| Strict / lenient, model's label | 93% / 93% | 67% / 87% | 67% / 93% |
| Strict / lenient, label from score | 93% / 93% | 67% / 93% | 67% / 93% |
| Evidence grounded | | not checked | 15/15 |
| Confidence given | | | high 14, medium 1 |

The 15 seeded cases have no incident context, so here v4 differs from v2 only in the output format.

**Reading it:**

- **v4 is worse on the legitimate side.** It catches all fraud, against 98%, but clears 69% of legitimate incidents against 77% and blocks 17% of good customers against 12%. Strict accuracy drops 4 points and flips rise from 7% to 10%. Most of the extra blocks are ordinary incidents (12 of 189 against 3), which looks like seeing the rule names pushing scores up. That's a guess from the counts; the traces haven't been checked.
- **Confidence carries almost no signal on this model.** It said `high` on 98% of verdicts, including 63 of the 65 it got wrong. H3's `low_confidence` trigger would almost never fire under v4, and under v2 every row is stored as `low`, so it would fire on every case.
- **The grounding check works but rarely fires.** Of the 2 flagged verdicts, one cited `context_from_detection_pipeline` as if it were a tool, and the other cited a `get_geo_consistency` call that had returned an error (wrong transaction id).
- v2 stays the default. The choice for H3 is between v4's grounding and confidence fields, and v2's better clearing of legitimate customers.
