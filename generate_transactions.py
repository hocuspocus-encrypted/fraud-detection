"""
generate_transactions.py

Standalone synthetic transaction generator for the real-time fraud
detection pipeline. This does NOT talk to Kafka yet — that comes in the
ingestion step. For now it just proves out the data shape and the
"live stream" feel by printing (and optionally saving) one JSON record
at a time on an interval.

Usage:
    pip install faker
    python generate_transactions.py --count 20 --interval 0.5
    python generate_transactions.py --count 100 --interval 0 --out transactions.jsonl
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "dining", "fuel",
    "entertainment", "healthcare", "clothing", "utilities", "online_retail",
]

CARD_TYPES = ["visa", "mastercard", "amex", "discover"]

# Small base fraud rate, bumped up for a few "risky" patterns so your
# downstream model has real signal to learn from later.
BASE_FRAUD_RATE = 0.02


def generate_transaction() -> dict:
    amount = round(random.lognormvariate(mu=3.5, sigma=1.1), 2)  # skewed, mostly small amounts
    category = random.choice(MERCHANT_CATEGORIES)
    hour = datetime.now(timezone.utc).hour

    # Slightly elevate fraud probability for late-night, high-amount, or online_retail txns
    fraud_prob = BASE_FRAUD_RATE
    if amount > 800:
        fraud_prob += 0.15
    if hour < 5 or hour > 23:
        fraud_prob += 0.10
    if category == "online_retail":
        fraud_prob += 0.05

    is_fraud = random.random() < min(fraud_prob, 0.9)

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1, 5000):05d}",
        "amount": amount,
        "currency": "USD",
        "merchant": fake.company(),
        "merchant_category": category,
        "card_type": random.choice(CARD_TYPES),
        "card_last4": f"{random.randint(0, 9999):04d}",
        "location_city": fake.city(),
        "location_country": fake.country_code(),
        "is_fraud": is_fraud,  # ground-truth label for later model evaluation
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction records.")
    parser.add_argument("--count", type=int, default=50, help="Number of records to generate (0 = infinite).")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds to sleep between records.")
    parser.add_argument("--out", type=str, default=None, help="Optional path to append JSON lines to.")
    args = parser.parse_args()

    out_file = open(args.out, "a") if args.out else None

    generated = 0
    try:
        while args.count == 0 or generated < args.count:
            record = generate_transaction()
            line = json.dumps(record)
            print(line)
            if out_file:
                out_file.write(line + "\n")
                out_file.flush()
            generated += 1
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {generated} records.")
    finally:
        if out_file:
            out_file.close()


if __name__ == "__main__":
    main()
