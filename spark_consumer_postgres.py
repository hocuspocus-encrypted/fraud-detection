"""
spark_consumer_postgres.py

Structured Streaming job that reads transactions from Kafka, parses the
JSON, and writes each micro-batch to PostgreSQL via JDBC using
foreachBatch. Also prints a preview to console so you can watch it work.

Run with (Java 17 active, HADOOP_HOME set, in this terminal session):
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.4 \
        spark_consumer_postgres.py

Note: the Postgres JDBC driver (org.postgresql:postgresql) is a SEPARATE
package from the Kafka connector -- both need to be listed, comma-separated,
in --packages. Forgetting the JDBC driver is a common cause of
"No suitable driver" errors at write time.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

# Matches docker-compose.yml postgres service credentials
JDBC_URL = "jdbc:postgresql://localhost:5432/fraud_db"
JDBC_PROPERTIES = {
    "user": "fraud_user",
    "password": "fraud_pass",
    "driver": "org.postgresql.Driver",
}
TARGET_TABLE = "transactions"

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
    """Called once per micro-batch by foreachBatch. batch_df is a regular
    (non-streaming) DataFrame here, so normal JDBC write APIs work fine."""
    row_count = batch_df.count()
    if row_count == 0:
        return  # nothing to do this batch

    # Convert the ISO-8601 string timestamp into a real Spark TimestampType.
    # Without this, the JDBC driver sends it as plain text, which Postgres
    # refuses to implicitly cast into a timestamptz column.
    to_write = (
        batch_df
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .select(
            "transaction_id", "event_timestamp", "user_id", "amount", "currency",
            "merchant", "merchant_category", "card_type", "card_last4",
            "location_city", "location_country", "is_fraud",
        )
    )

    (
        to_write.write
        .mode("append")
        .jdbc(url=JDBC_URL, table=TARGET_TABLE, properties=JDBC_PROPERTIES)
    )
    print(f"[batch {batch_id}] wrote {row_count} rows to Postgres")


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
