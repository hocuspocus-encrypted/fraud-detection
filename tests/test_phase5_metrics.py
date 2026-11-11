"""
Phase 5 checks for the metrics script itself (PLAN.md, Phase 5): rows with
known timestamps in a throwaway database must give the expected latencies,
queue depth, failure counts and Spark-health buckets.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
from datetime import datetime, timedelta, timezone

import pytest

from db import connect
from db.migrate import migrate, recreate_database
from phase5_metrics import (agent_vs_ground_truth, failure_summary, latency_stats, outstanding_series,
                            percentile, spark_health, stream_gaps)

METRICS_DB = "fraud_phase5_test"
T0 = datetime(2026, 10, 22, 10, 0, tzinfo=timezone.utc)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


@pytest.fixture(scope="module")
def conn():
    recreate_database(METRICS_DB)
    migrate(METRICS_DB)
    c = connect("admin", METRICS_DB)
    c.autocommit = True
    with c.cursor() as cur:
        # Three incidents, one transaction each, for users a/b/c.
        # a: txn at 0, queued at 6, claimed at 10, done at 10.5
        # b: txn at 1, queued at 7, claimed at 20, done at 21 (verdict legitimate; was fraud)
        # c: txn at 2, queued at 8, failed at 9 (never claimed successfully)
        for user, minute, fraud in (("a", 0, False), ("b", 1, True), ("c", 2, False)):
            cur.execute(
                """INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency,
                                             merchant, is_fraud, ingested_at)
                   VALUES (%s, %s, %s, 500, 'USD', 'M', %s, %s)""",
                (f"t_{user}", at(minute), f"cust_{user}", fraud, at(minute) + timedelta(seconds=10)),
            )
            if fraud:
                cur.execute("INSERT INTO ground_truth_labels (label_type, pattern_id, user_id, transaction_id, details) "
                            "VALUES ('card_testing', 'p1', %s, %s, '{\"is_fraud\": true}')", (f"cust_{user}", f"t_{user}"))
            cur.execute(
                "INSERT INTO anomaly_flags (user_id, window_start, window_end, txn_count, total_amount, detected_at) "
                "VALUES (%s, %s, %s, 1, 500, %s)", (f"cust_{user}", at(minute - 1), at(minute + 1), at(minute + 1)))
        rows = [
            ("cust_a", at(0), "done", at(6), at(10), at(10.5), "likely_fraud"),
            ("cust_b", at(1), "done", at(7), at(20), at(21), "legitimate"),
            ("cust_c", at(2), "failed", at(8), at(8.5), at(9), None),
        ]
        for user, txn_time, status, created, claimed, finished, verdict in rows:
            cur.execute(
                """INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids, primary_txn_id,
                                               status, attempts, claimed_at, finished_at, verdict, created_at)
                   VALUES (%s, %s, %s, '{1}', ARRAY[%s]::varchar[], %s, %s, 1, %s, %s, %s, %s) RETURNING id""",
                (user, txn_time - timedelta(minutes=1), txn_time + timedelta(minutes=1), f"t_{user[-1]}",
                 f"t_{user[-1]}", status, claimed, finished, verdict, created),
            )
            iid = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO investigation_attempts (investigation_id, attempt, started_at, finished_at, outcome) "
                "VALUES (%s, 1, %s, %s, %s)", (iid, claimed, finished, "done" if status == "done" else "failed"))
    yield c
    c.close()
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{METRICS_DB}" WITH (FORCE)')
    admin.close()


def test_percentile():
    assert percentile([], 0.5) is None
    assert percentile([5], 0.95) == 5
    assert percentile([1, 2, 3, 4], 0.5) == 2.5
    assert percentile(list(range(1, 101)), 0.95) == pytest.approx(95.05)


def test_latency_stages(conn):
    s = latency_stats(conn, at(-5), at(60))
    assert s["n"] == 2                              # only done incidents count
    assert s["total"]["p50"] == pytest.approx(15.25 * 60)   # a: 10.5 min, b: 20 min
    assert s["settle"]["p50"] == pytest.approx(6 * 60)      # first txn -> queued: 6 and 6
    assert s["queue_wait"]["p50"] == pytest.approx(8.5 * 60)  # 4 and 13
    assert s["investigation"]["p50"] == pytest.approx(0.75 * 60)  # 0.5 and 1


def test_outstanding_series(conn):
    series = dict(outstanding_series(conn, at(0), at(25), step_minutes=5))
    assert series[at(5)] == 0       # nothing queued yet
    assert series[at(10)] == 2      # a claimed but not done, b waiting; c failed at 9
    assert series[at(15)] == 1      # b
    assert series[at(25)] == 0


def test_failure_summary(conn):
    f = failure_summary(conn, at(-5), at(60))
    assert f["by_status"] == {"done": 2, "failed": 1}
    assert f["failure_rate"] == pytest.approx(1 / 3)
    assert f["attempt_outcomes"] == {"done": 2, "failed": 1}


def test_spark_health_buckets(conn):
    buckets = spark_health(conn, at(0), at(10), bucket_minutes=5)
    first = buckets[0]
    assert first["bucket"] == at(0)
    assert first["txns"] == 3 and first["ingest_lag_p50"] == pytest.approx(10.0)
    assert first["flag_rows"] == 3                  # detected at minutes 1, 2, 3
    assert buckets[1]["txns"] == 0 and buckets[1]["flag_rows"] == 0


def test_stream_gaps_flag_a_suspended_machine(conn):
    """Transactions were ingested at minutes 0, 1, 2 (+10 s). A run where the stream stops
    for longer than the threshold (e.g. the laptop slept) must be reported as invalid."""
    assert stream_gaps(conn, at(0), at(5), threshold_seconds=90) == []
    gaps = stream_gaps(conn, at(0), at(5), threshold_seconds=50)
    assert [round(seconds) for _, _, seconds in gaps] == [60, 60]


def test_agent_vs_ground_truth(conn):
    m = agent_vs_ground_truth(conn, at(-5), at(60))
    assert m[("fraud", "legitimate")] == 1          # b: labeled fraud, cleared by the agent
    assert m[("no_fraud", "likely_fraud")] == 1     # a: no fraud label, blocked
