# Real-Time Financial Fraud Detection — Ingestion Pipeline

## Phase: Days 1–2 — Real-Time Ingestion

This phase builds the full ingestion backbone of the fraud detection system: a synthetic transaction stream, a Kafka broker to carry it, and a Spark Structured Streaming job that lands parsed, typed data into PostgreSQL. By the end of this phase, transactions flow continuously from generation through to durable, queryable storage.

---

## Architecture

```
generate_transactions.py  →  Kafka topic "transactions"  →  Spark Structured Streaming  →  PostgreSQL
   (synthetic data)             (kafka_producer.py)            (spark_consumer_postgres.py)   (transactions table)
```

All infrastructure (Kafka, Postgres, Kafka UI) runs locally via Docker Compose.

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

---

## Next Phase

With ingestion fully working, the natural next step is the fraud detection logic itself — either rule-based flagging (velocity checks, amount thresholds) built into the Spark job, or training an actual ML model on the accumulated Postgres data, in line with the project's applied-AI goals.
