# Real-Time Financial Fraud Detection

## Phase: Days 1–2 — Real-Time Ingestion

This phase builds the full ingestion backbone of the fraud detection system: a synthetic transaction stream, a Kafka broker to carry it, and a Spark Structured Streaming job that lands parsed, typed data into PostgreSQL. By the end of this phase, transactions flow continuously from generation through to durable, queryable storage.

## Phase: Day 3 — Rolling Anomaly Detection

Building on ingestion, `spark_fraud_detection.py` computes rolling per-user transaction frequency and spend using sliding time windows (2-minute window, 30-second slide), flags users whose activity crosses simple thresholds (`> 5` transactions or `> $1000` spend in-window), and upserts flagged windows into a dedicated `anomaly_flags` table in Postgres.

## Phase: Day 4 — Live Dashboard

`streamlit_dashboard.py` reads directly from Postgres (both `transactions` and `anomaly_flags`) and auto-refreshes every few seconds, so the pipeline's output is visible as it happens — top-line metrics, transaction volume over time, recent transactions, flagged anomalies, and spend by merchant category.

---

## Architecture

```
generate_transactions.py  →  Kafka topic "transactions"  →  Spark Structured Streaming  →  PostgreSQL
   (synthetic data)             (kafka_producer.py)         ┬─ spark_consumer_postgres.py  → transactions table  ─┐
                                                              └─ spark_fraud_detection.py    → anomaly_flags table ─┴─→ streamlit_dashboard.py
```

All infrastructure (Kafka, Postgres, Kafka UI) runs locally via Docker Compose.

---

## How to Run

1. **Start infrastructure** (Kafka, Postgres, Kafka UI):
   ```
   docker compose up -d
   ```

2. **Start the consumer(s) you want** (each in its own terminal window — both can run at once, against the same Kafka topic, independently):
   ```
   run_consumer_postgres.bat     REM raw transactions → transactions table
   run_fraud_detection.bat       REM rolling anomaly detection → anomaly_flags table
   ```
   For each, wait for its first-batch line (`[batch 0] wrote 0 rows...` / `[batch 0] total_rows=0 anomalies=0`) before moving on — that confirms the streaming query is up and stable.

   > **Don't start the producer before you see that line.** The Kafka source uses `startingOffsets: latest`, and that starting offset is locked in at the query's *first internal poll* — a few seconds after `.start()`, not at query-definition time. Data produced before that poll is silently treated as "old" and never processed. Always let batch 0 commit first.

3. **Start the transaction generator** (in its own terminal window):
   ```
   run_producer.bat
   ```
   Streams synthetic transactions into Kafka continuously.

4. **Check results**:
   - **Live dashboard**: `run_dashboard.bat`, then open `http://localhost:8501` — auto-refreshes every few seconds with transaction volume, recent transactions, and flagged anomalies straight from Postgres.
   - **`psql` in the Postgres container**:
     ```
     docker exec -it fraud_postgres psql -U fraud_user -d fraud_db -c "SELECT * FROM anomaly_flags ORDER BY detected_at DESC;"
     ```
   - Or browse Kafka topics/messages visually at `http://localhost:8080` (Kafka UI).

5. **Stop everything**: Ctrl+C each open window (consumers, producer, dashboard), then `docker compose down` (add `-v` only if you also want to wipe Kafka/Postgres data).

### Troubleshooting
- **`FileAlreadyExistsException` in the logs, or the consumer sits idle forever with no batches progressing**: check `tasklist | findstr java` — if more than one `java.exe` is running, a stale process from a previous session is fighting the new one over the same checkpoint. Kill the extras, delete the affected `checkpoints/<name>/` directory, and restart.
- **Every batch takes 60–100+ seconds even with almost no data**: this was Spark's default `spark.sql.shuffle.partitions=200` causing massive per-batch overhead on `local[*]`. Already fixed in `spark_fraud_detection.py` (set to `4`) — if you see this again, check that config wasn't reverted.

---

## Stack

| Component | Technology | Version |
|---|---|---|
| Containerization | Docker Desktop | — |
| Message broker | Apache Kafka (official image, KRaft mode) | 3.8.0 |
| Broker UI | Kafka UI (Provectus) | latest |
| Database | PostgreSQL | 16 |
| Stream processing | PySpark Structured Streaming | 4.1.1 |
| Kafka client (producer) | kafka-python-ng | latest |
| JDBC driver | org.postgresql:postgresql | 42.7.4 |
| Runtime | Java (Temurin) | 17.0.19 |
| Language | Python | (venv) |

---

## What Was Built

### 1. Docker environment (`docker-compose.yml`)
Three containers, networked together:
- **Kafka** — single-node broker running in KRaft mode (no Zookeeper needed), with dual listeners: one for other containers (`fraud_kafka:19092`) and one exposed to the host machine (`localhost:9092`).
- **Kafka UI** — browser dashboard at `localhost:8080` for inspecting topics and messages visually.
- **PostgreSQL** — database `fraud_db`, user `fraud_user`, exposed on `localhost:5432`.

### 2. Synthetic transaction generator (`generate_transactions.py`)
A standalone Python script producing realistic transaction-style JSON records: transaction ID, timestamp, user ID, amount (log-normally distributed, mostly small with a long tail), merchant, category, card details, location, and an `is_fraud` label. Fraud probability is deliberately elevated for large amounts, late-night activity, and online retail purchases, so the data has real learnable signal for later modeling — not just random noise.

### 3. Kafka producer (`kafka_producer.py`)
Streams generated transactions onto the `transactions` Kafka topic in real time, keyed by `user_id` (so all of one user's transactions land on the same partition, preserving order — useful for future per-user pattern detection).

### 4. Database schema (`schema.sql`)
A `transactions` table in Postgres mirroring the transaction shape, with indexes on `user_id`, `event_timestamp`, and `is_fraud` to support the kinds of queries a fraud system will eventually need (per-user activity, time-windowed analysis, fraud-rate reporting).

### 5. Spark Structured Streaming consumer (`spark_consumer_postgres.py`)
Reads continuously from the Kafka topic, parses the JSON payload into a typed DataFrame, casts the timestamp field to a proper `TimestampType`, and writes each micro-batch to Postgres via JDBC using `foreachBatch`. Includes checkpointing (so it can resume safely after a restart without reprocessing or losing data) and an auto-restart loop to recover from a known Spark 4.1.1 internal metrics bug without losing the stream.

### 6. Supporting files
- `spark_consumer.py` — an earlier console-only version of the consumer (Kafka → console), kept as a simpler reference/debugging tool.

### 7. Rolling anomaly detection (`spark_fraud_detection.py`)
Reads the same Kafka topic independently, applies a watermarked sliding-window aggregation (`groupBy(window(...), user_id)`) to compute per-user transaction count and total spend over a 2-minute window (recomputed every 30 seconds), flags windows crossing simple thresholds, and upserts flagged rows into `anomaly_flags` via `psycopg2` (not plain JDBC, to get `ON CONFLICT` upsert semantics — a window re-emitted on a later trigger with updated counts overwrites its previous row instead of duplicating). Uses Spark's built-in windowed aggregation rather than a hand-rolled stateful operator, so there's no manual state-update logic to maintain.

---

## Result

A live, growing `transactions` table in Postgres, populated in real time from a simulated transaction stream — confirmed with over 1,700 rows and climbing, with correct types (`timestamp with time zone`, `numeric`, `boolean`) throughout.

```sql
SELECT transaction_id, event_timestamp, amount, is_fraud
FROM transactions
ORDER BY event_timestamp DESC
LIMIT 5;
```

---

## Problems Hit and Fixed Along the Way

This pipeline touched a lot of real infrastructure, and almost every layer had at least one genuine issue to diagnose:

1. **Bitnami image paywall** — `bitnami/kafka` images were moved behind a paid "Secure Images" subscription in August 2025; free tags were removed. Fixed by switching to the official `apache/kafka` image (KRaft-native, no Zookeeper).
2. **`kafka-python` broken on modern Python** — the original `kafka-python` library has an unfixed compatibility bug with Python 3.12+ (`kafka.vendor.six.moves` fails to import). Fixed by switching to the community-maintained fork `kafka-python-ng`, a drop-in replacement.
3. **Windows Store Python alias interference** — `spark-submit` was silently resolving `python`/`python3` to Windows' non-functional Store alias instead of the venv's real interpreter. Fixed by disabling the App Execution Aliases and explicitly setting `PYSPARK_PYTHON`.
4. **Java version mismatch** — the system had Java 25 installed, newer than PySpark supports (17 or 21). Fixed by installing Java 17 alongside the existing install and scoping `JAVA_HOME` to the terminal session rather than changing the system default.
5. **Missing Hadoop native libraries on Windows** — Spark's bundled Hadoop client needs `winutils.exe`/`hadoop.dll` even for purely local (non-HDFS) use on Windows. Fixed by sourcing correct binaries from the community `cdarlint/winutils` repo (after an initial bad download and an incorrect version-path assumption were caught and corrected).
6. **Docker Desktop hostname resolution quirk** — the machine's hostname was resolving to `host.docker.internal` / `kubernetes.docker.internal`, breaking Spark's internal driver–executor communication even in local mode. Fixed with explicit `spark.driver.host`/`bindAddress` configs and `SPARK_LOCAL_HOSTNAME`/`SPARK_LOCAL_IP` environment variables forcing `127.0.0.1`.
7. **Timestamp type mismatch in JDBC write** — the JSON `timestamp` field was written to Postgres as a raw string, which Postgres refused to implicitly cast into its `timestamptz` column. Fixed by explicitly casting to Spark's `TimestampType` (`to_timestamp`) before the JDBC write.
8. **Spark 4.1.1 internal metrics bug** — a `NullPointerException` inside Spark's own Kafka offset-metrics reporting code intermittently crashed the streaming query even after a successful batch write. Fixed pragmatically with an auto-restart loop around the query, since checkpointing guarantees no data loss or duplication across restarts.
9. **Two driver processes racing on the same checkpoint** — a stale process left running from an earlier session, plus a freshly launched one, both wrote to `checkpoints/anomaly_flags/`, causing `FileAlreadyExistsException` on state-store checkpoint files and preventing any batch from ever committing. Fixed by always confirming `tasklist | findstr java` shows only one instance before starting, and clearing the checkpoint directory if a corrupted (never-committed) one is found.
10. **Default `spark.sql.shuffle.partitions=200` made local testing impractically slow** — every windowed-aggregation batch spun up 200 tiny state-store partitions on local disk regardless of data volume, taking 90–180+ seconds per batch even for a handful of test rows. Fixed by setting `spark.sql.shuffle.partitions=4` in the `SparkSession` builder for local runs, dropping batch time to ~7 seconds.
11. **`startingOffsets: "latest"` race with test data** — the Kafka source's actual starting offset is locked in at the query's *first internal poll*, a few seconds after `.start()`, not at query-definition time. Producing test data immediately after starting the query can land before that poll and get silently treated as pre-existing data, never processed by any batch. Fixed by always waiting for the first batch to commit (visible via a per-batch `total_rows`/`anomalies` debug print added to `write_anomalies_to_postgres`) before producing test transactions.

---

## Next Phase

With ingestion and rule-based rolling anomaly detection both working end-to-end (verified: a controlled burst of transactions for one test user correctly tripped both thresholds and landed in `anomaly_flags`), the natural next step is training an actual ML model on the accumulated Postgres data — using `is_fraud` as ground truth against real transaction features rather than simple velocity/amount rules — in line with the project's applied-AI goals.
