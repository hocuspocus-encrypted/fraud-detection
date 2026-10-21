"""
kafka_producer.py

Streams synthetic transactions (generate_transactions.TransactionGenerator)
into a Kafka topic, simulating a live transaction feed. The message format is
unchanged; see docs/CURRENT_STATE.md section 2.

Ground-truth labels for injected fraud and legitimate oddities go straight to
Postgres (ground_truth_labels, as pipeline_writer), never through Kafka, so the
pipeline and the agent can't see them. Run seed_reference_data.py once first
with the same --seed/--users so account_profiles and merchant_risk match.

Usage:
    pip install kafka-python-ng
    python kafka_producer.py --topic transactions --interval 0.5
    python kafka_producer.py --topic transactions --count 500 --interval 0
    python kafka_producer.py --no-labels      (don't write ground truth)
"""
import argparse
import json
import time

import psycopg2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from psycopg2.extras import Json

from db import connect
from generate_transactions import DEFAULT_SEED, DEFAULT_USERS, TransactionGenerator

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


def write_labels(conn, labels: list[dict]) -> None:
    with conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO ground_truth_labels (label_type, pattern_id, user_id, transaction_id, details)
               VALUES (%s, %s, %s, %s, %s)""",
            [(lb["label_type"], lb["pattern_id"], lb["user_id"], lb["transaction_id"], Json(lb["details"]))
             for lb in labels],
        )


def main():
    parser = argparse.ArgumentParser(description="Stream synthetic transactions into Kafka.")
    parser.add_argument("--topic", type=str, default="transactions", help="Kafka topic to produce to.")
    parser.add_argument("--count", type=int, default=0, help="Number of records to send (0 = infinite).")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds to sleep between ticks.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    parser.add_argument("--no-labels", action="store_true", help="Don't write ground_truth_labels.")
    args = parser.parse_args()

    try:
        producer = build_producer()
    except NoBrokersAvailable:
        print(f"Could not reach Kafka at {BOOTSTRAP_SERVERS}. Is `docker compose up -d` running?")
        return

    labels_conn = None if args.no_labels else connect("pipeline_writer")
    gen = TransactionGenerator(seed=args.seed, n_users=args.users)
    print(f"Producing to topic '{args.topic}' at {BOOTSTRAP_SERVERS} "
          f"(seed {args.seed}, {args.users} customers) ... (Ctrl+C to stop)")

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            labels = []
            for record, label in gen.step():
                future = producer.send(args.topic, key=record["user_id"], value=record)
                future.add_callback(on_send_success, transaction_id=record["transaction_id"])
                future.add_errback(on_send_error)
                sent += 1
                if label:
                    labels.append(label)
            if labels and labels_conn is not None:
                try:
                    write_labels(labels_conn, labels)
                except psycopg2.Error as e:
                    # Keep the stream going; missing labels only weaken later evaluation.
                    print(f"WARNING: {len(labels)} ground-truth label(s) not written: {e}")
                    labels_conn.close()
                    labels_conn = connect("pipeline_writer")
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {sent} records.")
    finally:
        producer.flush()
        producer.close()
        if labels_conn is not None:
            labels_conn.close()


if __name__ == "__main__":
    main()
