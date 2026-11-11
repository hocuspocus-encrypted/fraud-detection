"""
simulate_flags.py

Offline estimate of what the Spark rules (spark_fraud_detection.py) will flag
on the generator's stream, without running Kafka or Spark. Used to choose
the generator's defaults (docs/CURRENT_STATE.md section 10).

It reproduces the streaming logic: each transaction falls in the 4 sliding
windows (2 min long, 30 s slide) that contain it; a window is flagged at more
than FREQUENCY_THRESHOLD transactions or more than SPEND_THRESHOLD total; a
user's overlapping or touching flagged windows merge into one incident, as in
enqueue_incidents(). Incidents are scored against the generator's labels.
Watermarks and late data are ignored.

Usage:
    venv\\Scripts\\python.exe simulate_flags.py                       (current defaults)
    venv\\Scripts\\python.exe simulate_flags.py --users 5000 --interval 1.0
"""
import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import generate_transactions as gt

# Must match spark_fraud_detection.py; deliberately not imported (that module needs pyspark).
FREQUENCY_THRESHOLD = 2
SPEND_THRESHOLD = 300.0
WINDOW_SECONDS, SLIDE_SECONDS = 120, 30

PROFILE_PARAMS = ("SPEND_MU_MEAN", "SPEND_MU_SD", "SPEND_SIGMA_RANGE", "ACTIVITY_SIGMA")


SIM_START = datetime(2026, 10, 21, 0, 0, tzinfo=timezone.utc)


def generate_stream(seed: int = gt.DEFAULT_SEED, n_users: int = gt.DEFAULT_USERS, interval: float = 0.5,
                    hours: float = 1.0, stream_seed: int | None = None, start: datetime = SIM_START,
                    **profile_params) -> tuple:
    """Step a generator on a fake clock, like the producer does. Returns (generator, [(record, label)])."""
    saved = {k: getattr(gt, k) for k in profile_params}
    for k, v in profile_params.items():
        assert k in PROFILE_PARAMS, k
        setattr(gt, k, v)
    try:
        now = [start]
        gen = gt.TransactionGenerator(seed=seed, n_users=n_users, clock=lambda: now[0], stream_seed=stream_seed)
        events = []
        for _ in range(int(hours * 3600 / interval)):
            events.extend(gen.step())
            now[0] += timedelta(seconds=interval)
    finally:
        for k, v in saved.items():
            setattr(gt, k, v)
    return gen, events


def find_incidents(events) -> list[tuple]:
    """(user_id, start_epoch, end_epoch) per incident, as Spark + enqueue_incidents() would form them."""
    windows = defaultdict(lambda: [0, 0.0])
    for record, _ in events:
        t = datetime.fromisoformat(record["timestamp"]).timestamp()
        first = int(t // SLIDE_SECONDS) * SLIDE_SECONDS
        for k in range(WINDOW_SECONDS // SLIDE_SECONDS):
            w = windows[(record["user_id"], first - k * SLIDE_SECONDS)]
            w[0] += 1
            w[1] += record["amount"]

    flagged = defaultdict(list)
    for (user, s), (count, total) in windows.items():
        if count > FREQUENCY_THRESHOLD or total > SPEND_THRESHOLD:
            flagged[user].append(s)

    incidents = []
    for user, starts in flagged.items():
        starts.sort()
        cur_start, cur_end = starts[0], starts[0] + WINDOW_SECONDS
        for s in starts[1:]:
            if s > cur_end:
                incidents.append((user, cur_start, cur_end))
                cur_start = s
            cur_end = max(cur_end, s + WINDOW_SECONDS)
        incidents.append((user, cur_start, cur_end))
    return sorted(incidents, key=lambda i: (i[2], i[0]))


def simulate(seed: int = gt.DEFAULT_SEED, n_users: int = gt.DEFAULT_USERS, interval: float = 0.5,
             hours: float = 1.0, **profile_params) -> dict:
    _, events = generate_stream(seed, n_users, interval, hours, **profile_params)
    by_user = defaultdict(list)
    for record, label in events:
        by_user[record["user_id"]].append((datetime.fromisoformat(record["timestamp"]).timestamp(), record, label))
    incidents = find_incidents(events)

    kinds = defaultdict(int)
    detected_patterns = set()
    for user, s, e in incidents:
        inside = [(rec, lab) for t, rec, lab in by_user[user] if s <= t < e]
        fraud = [lab for rec, lab in inside if rec["is_fraud"]]
        if fraud:
            kinds["fraud"] += 1
            detected_patterns.update(lab["pattern_id"] for lab in fraud)
        elif any(lab for _, lab in inside):
            kinds["legit_oddity"] += 1
        else:
            kinds["normal_behavior"] += 1

    patterns = {}
    for _, label in events:
        if label and label["details"]["is_fraud"]:
            patterns[label["pattern_id"]] = label["label_type"]
    recall = {}
    for kind in gt.FRAUD_KINDS:
        ids = [p for p, k in patterns.items() if k == kind]
        recall[kind] = (sum(p in detected_patterns for p in ids), len(ids))

    amounts = sorted(r["amount"] for r, label in events if not label)
    per_hour = 1 / hours
    return {
        "incidents_per_hour": round(len(incidents) * per_hour),
        "fraud": round(kinds["fraud"] * per_hour), "legit_oddity": round(kinds["legit_oddity"] * per_hour),
        "normal_behavior": round(kinds["normal_behavior"] * per_hour),
        "precision": round(kinds["fraud"] / len(incidents), 3) if incidents else None,
        "recall": recall,
        "median_amount": amounts[len(amounts) // 2], "p95_amount": amounts[int(len(amounts) * 0.95)],
        "txns_per_hour": round(len(events) * per_hour),
    }


def main():
    parser = argparse.ArgumentParser(description="Estimate Spark flag volume on the generator's stream.")
    parser.add_argument("--seed", type=int, default=gt.DEFAULT_SEED)
    parser.add_argument("--users", type=int, default=gt.DEFAULT_USERS)
    parser.add_argument("--interval", type=float, default=0.5, help="Producer seconds per tick.")
    parser.add_argument("--hours", type=float, default=1.0)
    args = parser.parse_args()
    print(simulate(args.seed, args.users, args.interval, args.hours))


if __name__ == "__main__":
    main()
