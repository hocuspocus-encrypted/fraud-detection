# Current State (Phase 0 recon)

Recorded 2026-10-19 from the repo, the running containers, and a live smoke test. Phase 0 changed no code.

## 1. Components

| Piece | Where it runs | Source |
|---|---|---|
| Kafka 3.8.0 (KRaft, single node) | Docker `fraud_kafka`, host port 9092, in-network `fraud_kafka:19092` | `docker-compose.yml` |
| Kafka UI | Docker `fraud_kafka_ui`, port 8080 (image `:latest`, unpinned) | `docker-compose.yml` |
| Postgres 16 | Docker `fraud_postgres`, port 5432, db `fraud_db`, user `fraud_user` / `fraud_pass` | `docker-compose.yml` |
| Producer | Host, `venv` Python 3.13.9 | `kafka_producer.py` + `generate_transactions.py` |
| Raw ingest Spark job | Host, Spark 4.1.1 `local[*]`, Java 17 | `spark_consumer_postgres.py`, checkpoint `./checkpoints/transactions_to_postgres` |
| Anomaly Spark job | Host, same | `spark_fraud_detection.py`, checkpoint `./checkpoints/anomaly_flags` |
| Dashboard | Host, Streamlit 1.59 | `streamlit_dashboard.py` (+ `evaluation.py`) |

Only Kafka, Kafka UI, and Postgres are containerized. Both Spark jobs and Streamlit run on the host through the `.bat` launchers.

Relevant venv packages: `pyspark 4.1.1`, `kafka-python-ng 2.2.3`, `psycopg2-binary 2.9.12`, `SQLAlchemy 2.0.51`, `Faker 40.28.1`, `pandas 3.0.3`. The agent's dependencies (`google-genai`, `anthropic`, `python-dotenv`) are **not** installed in this venv.

## 2. Kafka

- Topic: `transactions`. 1 partition, replication factor 1, auto-created.
- Key: `user_id` (UTF-8). Value: JSON (UTF-8):

```json
{
  "transaction_id": "uuid4 string",
  "timestamp": "ISO-8601 with +00:00 offset (UTC)",
  "user_id": "user_00001 .. user_05000",
  "amount": 12.34,
  "currency": "USD",
  "merchant": "Faker company name",
  "merchant_category": "grocery|electronics|travel|dining|fuel|entertainment|healthcare|clothing|utilities|online_retail",
  "card_type": "visa|mastercard|amex|discover",
  "card_last4": "0000",
  "location_city": "Faker city",
  "location_country": "Faker country code",
  "is_fraud": false
}
```

## 3. Postgres schema (live, matches `schema.sql`)

```sql
CREATE TABLE transactions (
    transaction_id     VARCHAR(64) PRIMARY KEY,
    event_timestamp    TIMESTAMPTZ NOT NULL,
    user_id            VARCHAR(32) NOT NULL,
    amount             NUMERIC(12, 2) NOT NULL,
    currency           VARCHAR(8) NOT NULL,
    merchant           VARCHAR(255),
    merchant_category  VARCHAR(64),
    card_type          VARCHAR(32),
    card_last4         VARCHAR(4),
    location_city      VARCHAR(128),
    location_country   VARCHAR(8),
    is_fraud           BOOLEAN NOT NULL,          -- synthetic ground-truth label
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- indexes: pkey(transaction_id), (user_id), (event_timestamp), (is_fraud)

CREATE TABLE anomaly_flags (
    id              SERIAL PRIMARY KEY,
    user_id         VARCHAR(32) NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    txn_count       INTEGER NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, window_start, window_end)
);
-- indexes: pkey(id), unique(user_id, window_start, window_end), (user_id), (window_start)
```

Roles: only `fraud_user`, a **superuser**. There are no other roles.

Data on disk at recon time: 8,031 transactions (2026-07-16 and 2026-10-18/19), 762 anomaly flags.

## 4. How anomalies are decided

`spark_fraud_detection.py` uses sliding windows of 2 minutes, a 30 s slide, a 2-minute watermark, and `outputMode("update")`, grouped by `user_id`. A window is flagged when `txn_count > 2` **or** `total_amount > 300`. Flagged rows are upserted through psycopg2 with `ON CONFLICT (user_id, window_start, window_end) DO UPDATE`.

Note: `CONTEXT_FOR_CLAUDE_CODE.md` says the thresholds are 5 / 1000. The committed code (7df8e43) uses **2 / 300**. The code is the source of truth.

Consequences that matter for the integration:

- **The flag unit is a user window, not a transaction.** `anomaly_flags` has no `transaction_id`.
- **One event produces ~4 flag rows,** one for each overlapping 2-minute window it falls in. The live smoke test showed this (§6).
- **Rows are mutated after insert.** In update mode, a window can be emitted before it is complete and then upserted again with higher counts.
- In the Oct 18 data, 544 of 552 flags came from the spend rule alone, mostly one transaction over $300 (`txn_count = 1`).

## 5. Run / stop

```powershell
# start infra
docker compose up -d
# check nothing stale is holding the checkpoints (must print nothing)
tasklist | findstr /i java
# each in its own window; wait for "[batch 0]" before producing
.\run_consumer_postgres.bat
.\run_fraud_detection.bat
.\run_producer.bat        # --interval 0.5
.\run_dashboard.bat
# stop: Ctrl+C in each window, then make sure no java.exe is left
taskkill /F /IM java.exe
docker compose stop
```

The checkpoint paths are relative (`./checkpoints/...`), so the Spark jobs must start with the repo root as their working directory. The `.bat` launchers only work when run from the repo root.

Start the two Spark jobs a few seconds apart. On 2026-10-19, launching both in the same second left the raw consumer looping on `NullPointerException ... "idWithoutTopologyInfo" is null` (BlockManager registration) without ever processing a batch. Starting them 30 s apart fixed it with no config change. The same simultaneous start had worked earlier that day, so this looks intermittent. Also, neither job prints anything while no new data is arriving: the raw consumer logs only batches that wrote rows, and Spark runs no micro-batch at all without input. Silence is not a hang.

Added in Phase 1:

```powershell
# one-time: copy .env.example to .env and set the three role passwords
.\venv\Scripts\python.exe -m db.migrate          # apply pending migrations (safe to re-run)
.\run_enqueuer.bat                                # flags -> investigations, every 30 s
.\venv\Scripts\python.exe -m pytest -q            # Phase 1-3 checks (throwaway databases only)
```

Added in Phases 2-3:

```powershell
.\venv\Scripts\python.exe agent\eval.py --provider baseline   # eval on Postgres (throwaway fraud_agent_eval DB)
.\run_worker.bat                                              # investigate queued incidents (MODEL_PROVIDER in .env)
# reports land in the investigations row itself:
docker exec fraud_postgres psql -U fraud_user -d fraud_db -x -c "select id, user_id, verdict, risk_score, recommended_action, explanation from investigations where status = 'done' order by finished_at desc limit 5"
```

Added in Phase 4 (run once before producing, and again only if `--seed`/`--users` change):

```powershell
.\venv\Scripts\python.exe seed_reference_data.py   # merchant_risk + account_profiles for the generator (seed 42, 1000 customers)
```

The agent package lives in `agent/` (copied from the `fraud-agent` scaffold). Its tools read Postgres as `agent_ro`, using `psycopg2` with its own connection pool rather than psycopg 3 as the plan suggested, to stay on one driver. The original SQLite tool bodies are kept in `agent/tools_sqlite.py` as the parity reference.

## 6. Phase 0 verification

- `docker compose up -d` from stopped containers: OK.
- Both Spark jobs started from the launchers and resumed from their checkpoints. The known Spark 4.1.1 `KafkaMicroBatchStream.metrics` NPE appeared once and the restart loop recovered.
- Controlled burst: 8 × $200 for `user_phase0_test`. Result: 4 rows in `anomaly_flags` (windows 12:23:00 to 12:26:30 UTC), each with `txn_count = 8` and `total_amount = 1600.00`. All 8 raw rows landed in `transactions`. **The flag write path works.** In this run, the raw rows were ingested ~6.5 s before the flags were written, but the two jobs are independent and nothing guarantees that order.
- Alignment check over all flags:

| Flag date | Flags | Flags whose window contains the user's transactions |
|---|---|---|
| 2026-07-12 | 4 | 0 (test burst; raw consumer was not running) |
| 2026-07-16 | 202 | 0. All 202 line up if shifted by **+8 h**, which is the naive-datetime timezone bug |
| 2026-10-18/19 | 556 | 556, and `txn_count` matches the raw count every time |

The timezone fix (`.astimezone(timezone.utc)` in `spark_fraud_detection.py`) and `failOnDataLoss=false` in both jobs are **uncommitted**. So are the dashboard changes and `evaluation.py` (untracked). Why the July offset is 8 h and not the machine's current +4 h was not established. One guess is that the machine timezone was different then.

Test rows left behind: `user_phase0_test` (8 transactions, 4 flags).

## 7. Gaps between this pipeline and the agent tools

The agent tools (`fraud-agent/agent/tools.py`) expect SQLite tables `accounts(account_id, home_city, avg_monthly_spend, account_age_days)`, `transactions(txn_id, account_id, timestamp, amount, merchant, city, flagged)`, and `merchant_risk(merchant, risk_level, reason)`.

| # | Gap | Severity | Suggested fill |
|---|---|---|---|
| G1 | **No `flagged_transactions` table and no `txn_id` on flags.** Flags are overlapping user windows, ~4 per event, and they mutate after insert. A trigger on `anomaly_flags` INSERT would enqueue the same incident ~4 times, possibly before the window is complete. | Blocker for Phase 1 design | Decision needed, see §8 |
| G2 | **The generator has no per-account consistency.** In the live data, 8,023 transactions have 7,032 distinct merchants and 6,481 distinct cities. The 3,966 users average ~2 transactions each, and the most common merchant ("Smith LLC") appears 16 times. Home city, average spend, impossible-travel, merchant risk, and merchant-overlap similarity are all meaningless on this data. `get_geo_consistency` would report "different city" for nearly every transaction. | Blocker for useful live verdicts and for Phase 5 | Give the generator persistent per-user profiles (home city, typical spend, a merchant pool drawn from a finite catalog with popular and rare merchants), plus planted patterns (5a) |
| G3 | **Label leakage risk.** `transactions.is_fraud` is ground truth, and the agent must never see it. | Must-fix in Phase 1 | `agent_ro` reads only a view that omits `is_fraud` (for example `v_agent_transactions`), never the base table |
| G4 | Column names differ | Mechanical | `account_id`→`user_id`, `txn_id`→`transaction_id`, `timestamp`→`event_timestamp`, `city`→`location_city`. Map them in SQL (`AS`) so the tool schemas stay unchanged |
| G5 | No account profile data | Needed | `account_profiles`: `home_city` = most frequent `location_city`, `avg_monthly_spend` from history, `account_age_days` = days since first seen. History is short and sparse, so age will be small for everyone |
| G6 | No merchant risk data | Needed | `merchant_risk` table, seeded deterministically from a fixed catalog after G2. Unknown merchants stay "unknown" |
| G7 | Only a superuser role exists | Needed | `pipeline_writer`, `worker`, `agent_ro` (plan Phase 1) |
| G8 | Indexes missing for agent access patterns | Needed | `transactions(user_id, event_timestamp)`, `transactions(merchant, event_timestamp)` |
| G9 | Credentials are hardcoded in 5 files (`fraud_user`/`fraud_pass`) | Low (local only) | New code reads from `.env`. Leave existing pipeline files alone unless asked |
| G10 | The agent's dependencies aren't in the pipeline venv. Pipeline uses psycopg2, the plan suggests psycopg 3 | Low | Install `psycopg[binary,pool]`, `google-genai`, `python-dotenv` into the venv or into the worker image |
| G11 | The Spark jobs aren't in compose. The plan wants the worker as a compose service, so a containerized worker would connect to `fraud_postgres:5432`, not `localhost` | Decision | See §8 |
| G12 | Bad historical rows: 206 pre-October flags don't align | Low | Exclude flags before 2026-10-18, or truncate both tables before Phase 3 |

## 8. Decisions needed before Phase 1

1. **What gets investigated (G1).** Recommendation: investigate **incidents**, not window rows. An incident is the set of overlapping flagged windows for one user, merged. Its key is `(user_id, incident_start)`, and its transactions are those from `transactions` inside the merged span. Enqueue with a **settle delay** (window end + watermark, about 4-5 min) through a small enqueuer, not an INSERT trigger. That avoids duplicates, incomplete windows, and the race against the raw-ingest job, and the Spark jobs stay untouched. The alternative is switching Spark to tumbling windows, which changes a fragile job.
2. **Fix the generator (G2) before Phase 2's live use?** Recommended. Without it, every live investigation runs on noise. The SQLite eval parity in Phase 2 doesn't depend on it.
3. **Where the worker runs (G11).** On the host from the venv, which matches how everything else runs, or in a container as the plan says. Recommendation: host first, container later.
4. **Commit the uncommitted fixes** (timezone, `failOnDataLoss`, dashboard, `evaluation.py`) as their own commit before any Phase 1 work.

## 9. Repo hygiene noticed (not changed)

- Files under `checkpoints/transactions_to_postgres/commits/` were committed before `checkpoints/` was gitignored. Spark has since deleted them, so they show as `D` in `git status`. Fix: `git rm -r --cached checkpoints`.
- Git warns `LF will be replaced by CRLF`, and there is no `.gitattributes` yet. The plan asks for one, with LF for scripts used in containers.
- `fraud-agent/`: the ignore file is named **`gitignore` (no leading dot)**, so git ignores it, and `.env` (API keys) would be committed by `git add .`. That repo has no commits yet. Rename it to `.gitignore` before any commit there, or when copying into `agent/`.
- `CONTEXT_FOR_CLAUDE_CODE.md` is out of date on thresholds.

## 10. Phase 4: generator before and after (2026-10-21)

The generator (`generate_transactions.py`) now produces customers `cust_00001`-`cust_01000` with stable profiles from seed 42. The Kafka message format is unchanged. Older `user_*` rows from the random generator are still in the tables.

| Measure | Before (Phase 0 data, `user_*`) | After (30-min live run, `cust_*`) |
|---|---|---|
| Transactions | 8,023 | 3,600 |
| Distinct merchants | 7,032 | 278 (catalog of 300) |
| Distinct cities | 6,481 | 20 |
| Users seen / transactions per user | 3,966 / ~2.0 | 864 / 4.2 |
| Most common merchant | 16 transactions | 329 transactions |
| Median home-city share (users with 5+ txns) | n/a (no home city) | 1.000 |
| Users with 3+ txns who repeat a merchant | n/a | 304 of 496 (61%) |
| Fraud share | 2.4% (random per transaction) | 1.17% (labeled scenarios) |

Over 20,000 simulated steps (tests/test_phase4_generator.py), the median busy user made 100% of purchases at home and 93% at their own merchants.

**Flag precision on generated data is low.** In the first ~9 minutes, 55 incidents were flagged for generated customers: 3 contained injected fraud, 15 a labeled legitimate oddity, and 37 were ordinary behavior. The "more than 2 transactions in 2 minutes" rule fires on the busiest customers, because the stream compresses time (each customer buys something every few minutes). Over the full 30 minutes, 584 flag rows on 128 customers produced 178 incidents, about 350 an hour. That is more than one local-model worker clears (~170 an hour at ~21 s each).

**Consumer replay fix.** `spark_consumer_postgres.py` now writes with `INSERT ... ON CONFLICT (transaction_id) DO NOTHING` via psycopg2 instead of a JDBC append. Docker stopped on 2026-10-20 between batch 4988's write and its checkpoint commit; on restart the replayed batch hit the primary key on every retry (373 restarts, no progress). After the fix the replay wrote 0 of 24 rows and the job continued. All 3,600 transactions of the 30-minute run landed exactly once, through the replay, a mid-run kill, and one restart from the known metrics NPE.

### Generator defaults chosen with `simulate_flags.py` (2026-10-21)

`simulate_flags.py` reproduces the Spark windows, thresholds and incident merging offline. Checked against the live run: it predicted 182 incidents for the 30-minute run, against 178 observed. The Spark rules were not changed. Each row is one simulated hour at seed 42, varying one generator setting at a time:

| Generator setting | Incidents/h | With fraud | Legit oddity | Normal behavior | Precision | Caught (travel / burst / card test) | Median / p95 amount |
|---|---|---|---|---|---|---|---|
| A: first version, 1000 customers | 406 | 25 | 93 | 288 | 6% | 3/6, 14/14, 8/8 | $32 / $141 |
| B: 5000 customers | 159 | 17 | 45 | 97 | 11% | 1/6, 11/11, 5/5 | $33 / $142 |
| C: B + smaller usual amounts | 96 | 17 | 38 | 41 | 18% | 1/6, 11/11, 5/5 | $33 / $94 |
| **D: C + less activity skew (chosen)** | **59** | 18 | 21 | 20 | **31%** | 2/5, 9/9, 7/7 | $33 / $98 |
| E: D at 1 tick/s | 23 | 9 | 7 | 7 | 39% | 1/4, 4/4, 4/4 | $33 / $101 |

D on seeds 7 and 123: 60 and 75 incidents/h, 23% and 33% precision. The producer rate stays at 2 transactions/s. `tests/test_phase4_generator.py` re-simulates the defaults and fails if the volume leaves 30-120 incidents/h or precision drops below 15%.

**Limitation of the rules, not the generator:** the Spark rules catch impossible travel only by accident (0-3 of 4-9 patterns per simulated hour). Two ordinary-sized purchases in different countries trip neither rule, so the agent never sees most impossible-travel fraud.

**Reset for the new defaults.** All 936 queued incidents were deleted: 758 from the old random `user_` data, and 178 from the first 30-minute run. The first run's `cust_` data (3,600 transactions, 584 flags, 332 labels) was deleted too, since it was generated with the old amounts and activity. `ENQUEUE_SINCE` moved to 2026-10-21T09:56:18Z. Reference data was re-seeded for 5000 customers. The 42,771 older `user_` transactions are kept as history.
