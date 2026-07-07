"""
kafka_producer.py

Reads synthetic transaction records (via generate_transactions.py) and
publishes them to a Kafka topic, simulating a live transaction feed.

Usage:
    pip install kafka-python
    python kafka_producer.py --topic transactions --interval 0.5
    python kafka_producer.py --topic transactions --count 500 --interval 0
"""

import argparse
import json
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from generate_transactions import generate_transaction

BOOTSTRAP_SERVERS = "localhost:9092"  # matches PLAINTEXT_HOST listener from docker-compose.yml


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
    )


def on_send_success(record_metadata, transaction_id):
    print(
        f"sent {transaction_id} -> topic={record_metadata.topic} "
        f"partition={record_metadata.partition} offset={record_metadata.offset}"
    )


def on_send_error(excp):
    print(f"send failed: {excp}")


def main():
    parser = argparse.ArgumentParser(description="Stream synthetic transactions into Kafka.")
    parser.add_argument("--topic", type=str, default="transactions", help="Kafka topic to produce to.")
    parser.add_argument("--count", type=int, default=0, help="Number of records to send (0 = infinite).")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds to sleep between records.")
    args = parser.parse_args()

    try:
        producer = build_producer()
    except NoBrokersAvailable:
        print(f"Could not reach Kafka at {BOOTSTRAP_SERVERS}. Is `docker compose up -d` running?")
        return

    print(f"Producing to topic '{args.topic}' at {BOOTSTRAP_SERVERS} ... (Ctrl+C to stop)")

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            record = generate_transaction()
            future = producer.send(args.topic, key=record["user_id"], value=record)
            future.add_callback(on_send_success, transaction_id=record["transaction_id"])
            future.add_errback(on_send_error)
            sent += 1
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {sent} records.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
