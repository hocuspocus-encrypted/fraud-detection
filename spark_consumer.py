"""
spark_consumer.py

Structured Streaming job that reads synthetic transaction records from
the Kafka 'transactions' topic, parses the JSON payload into a typed
DataFrame, and streams the result to the console.

Run with (Java 17 active in this terminal session):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark_consumer.py

Note: the first run will download the Kafka connector + its dependencies
from Maven Central, which can take a minute or two.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

# Mirrors the record shape produced by generate_transactions.py
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


def main():
    spark = (
        SparkSession.builder
        .appName("FraudDetectionKafkaConsumer")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # Spark's default logging is very noisy

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")  # only new messages from now on
        .load()
    )

    # Kafka gives us key/value as bytes; decode value as a UTF-8 JSON string,
    # then parse it into structured columns using the schema above.
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), transaction_schema).alias("data"))
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
