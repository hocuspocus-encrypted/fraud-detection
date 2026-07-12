"""
spark_fraud_detection.py

Structured Streaming job that reads transactions from Kafka and computes
rolling, per-user transaction frequency and spend using sliding time
windows, flagging users whose activity crosses simple anomaly thresholds.

This uses Spark's built-in windowed aggregation (groupBy + window()) rather
than a custom stateful operator (mapGroupsWithState/flatMapGroupsWithState).
Spark manages the underlying state automatically -- there's no manual state
update logic to write, which is the "stateless transformation" framing:
each output row is a straightforward aggregation over a window, not a
hand-rolled running calculation.

Run with (Java 17 active, HADOOP_HOME set, in this terminal session):
    spark-submit \
        --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
        spark_fraud_detection.py
"""

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, sum as spark_sum, when
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

# Sliding window: look at a 2-minute rolling window, recomputed every 30s.
WINDOW_DURATION = "2 minutes"
SLIDE_DURATION = "30 seconds"
WATERMARK_DELAY = "2 minutes"  # how late a record can arrive and still count

# Simple anomaly thresholds -- tune these once you see real distributions.
FREQUENCY_THRESHOLD = 5       # more than 5 transactions in the window
SPEND_THRESHOLD = 1000.0      # more than $1000 total spend in the window

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


def write_anomalies_to_postgres(batch_df, batch_id):
    """Called once per micro-batch. Filters to only anomalous rows and
    upserts them into Postgres, so a window re-emitted on a later trigger
    (with updated counts) overwrites its previous row instead of duplicating."""
    total_rows = batch_df.count()
    anomalies = batch_df.filter(col("is_anomaly") == True).collect()  # noqa: E712
    print(f"[batch {batch_id}] total_rows={total_rows} anomalies={len(anomalies)}")
    if not anomalies:
        return

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            for row in anomalies:
                cur.execute(
                    """
                    INSERT INTO anomaly_flags
                        (user_id, window_start, window_end, txn_count, total_amount)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, window_start, window_end)
                    DO UPDATE SET
                        txn_count = EXCLUDED.txn_count,
                        total_amount = EXCLUDED.total_amount,
                        detected_at = now()
                    """,
                    (
                        row.user_id,
                        row.window_start,
                        row.window_end,
                        row.txn_count,
                        row.total_amount,
                    ),
                )
        conn.commit()
        print(f"[batch {batch_id}] upserted {len(anomalies)} anomaly rows")
    finally:
        conn.close()


def main():
    spark = (
        SparkSession.builder
        .appName("FraudDetectionRollingFrequency")
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
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), transaction_schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    )

    # Watermarking tells Spark how long to wait for late-arriving data before
    # finalizing a window. Without it, Spark would have to keep every
    # window's state forever, since it can never be sure no more data is
    # coming for an old window.
    windowed_activity = (
        parsed_stream
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            window(col("event_timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("user_id"),
        )
        .agg(
            count("*").alias("txn_count"),
            spark_sum("amount").alias("total_amount"),
        )
        .withColumn(
            "is_anomaly",
            (col("txn_count") > FREQUENCY_THRESHOLD) | (col("total_amount") > SPEND_THRESHOLD),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "txn_count",
            "total_amount",
            "is_anomaly",
        )
    )

    while True:
        query = (
            windowed_activity.writeStream
            .foreachBatch(write_anomalies_to_postgres)
            .outputMode("update")
            .option("checkpointLocation", "./checkpoints/anomaly_flags")
            .start()
        )
        try:
            query.awaitTermination()
            break
        except Exception as e:
            print(f"Streaming query crashed, restarting: {e}")
            continue


if __name__ == "__main__":
    main()
