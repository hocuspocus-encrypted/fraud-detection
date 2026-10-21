"""
spark_consumer_postgres.py

Structured Streaming job that reads transactions from Kafka, parses the
JSON, and writes each micro-batch to PostgreSQL using foreachBatch.

Writes are idempotent (INSERT ... ON CONFLICT (transaction_id) DO NOTHING via
psycopg2, the same pattern as spark_fraud_detection.py). Spark replays a
micro-batch whose output was written but whose commit wasn't checkpointed
(e.g. the process or Docker stopped in between); a plain JDBC append then hit
the primary key on every retry and the job never got past that batch
(2026-10-21, batch 4988, 373 restarts).

Run with (Java 17 active, HADOOP_HOME set, in this terminal session):
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.4 \
        spark_consumer_postgres.py

The Postgres JDBC package is no longer used by this job; it is left in the
launcher (run_consumer_postgres.bat) unchanged.
"""

from datetime import timezone

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

# Matches docker-compose.yml postgres service credentials
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "fraud_db",
    "user": "fraud_user",
    "password": "fraud_pass",
}

INSERT_SQL = """
    INSERT INTO transactions
        (transaction_id, event_timestamp, user_id, amount, currency, merchant,
         merchant_category, card_type, card_last4, location_city, location_country, is_fraud)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""

transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant", StringType()),
    StructField("merchant_category", StringType()),
    StructField("card_type", StringType()),
    StructField("card_last4", StringType()),
    StructField("location_city", StringType()),
    StructField("location_country", StringType()),
    StructField("is_fraud", BooleanType()),
])


def write_batch_to_postgres(batch_df, batch_id):
    """Called once per micro-batch by foreachBatch. Safe to call again with
    the same batch: rows already in Postgres are skipped."""
    # Convert the ISO-8601 string timestamp into a real Spark TimestampType.
    to_write = (
        batch_df
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .select(
            "transaction_id", "event_timestamp", "user_id", "amount", "currency",
            "merchant", "merchant_category", "card_type", "card_last4",
            "location_city", "location_country", "is_fraud",
        )
    )
    rows = to_write.collect()
    if not rows:
        return  # nothing to do this batch

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                INSERT_SQL,
                [
                    (
                        r.transaction_id,
                        # collect() returns naive datetimes in the machine's local time;
                        # make them tz-aware so Postgres doesn't read them as UTC.
                        r.event_timestamp.astimezone(timezone.utc),
                        r.user_id, r.amount, r.currency, r.merchant, r.merchant_category,
                        r.card_type, r.card_last4, r.location_city, r.location_country, r.is_fraud,
                    )
                    for r in rows
                ],
            )
            inserted = cur.rowcount  # executemany: total rows actually inserted
    finally:
        conn.close()
    print(f"[batch {batch_id}] wrote {inserted} of {len(rows)} rows to Postgres"
          + ("" if inserted == len(rows) else " (rest already present: replayed batch)"))


def main():
    spark = (
        SparkSession.builder
        .appName("FraudDetectionKafkaToPostgres")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        # Kafka deletes old messages after its retention period (7 days by
        # default). If the checkpoint points at offsets that no longer exist,
        # skip ahead to the earliest available data instead of crash-looping.
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), transaction_schema).alias("data"))
        .select("data.*")
    )

    # Spark 4.1.1's Kafka connector has a known flaky NullPointerException in
    # its internal progress-metrics calculation (KafkaMicroBatchStream.metrics)
    # that can crash the query even though the actual batch write succeeded.
    # Since our data writes are fine, we auto-restart the query if it dies,
    # rather than let one internal metrics bug take down the whole job.
    while True:
        query = (
            parsed_stream.writeStream
            .foreachBatch(write_batch_to_postgres)
            .outputMode("update")
            .option("checkpointLocation", "./checkpoints/transactions_to_postgres")
            .start()
        )
        try:
            query.awaitTermination()
            break  # clean stop (e.g. Ctrl+C), don't restart
        except Exception as e:
            print(f"Streaming query crashed, restarting: {e}")
            continue


if __name__ == "__main__":
    main()
