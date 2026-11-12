"""
Shared fixtures for the Phase 1 acceptance checks.

Every run builds a throwaway database (TEST_DB) from db/migrations, so the
checks never touch fraud_db. Roles are cluster-wide in Postgres, so the
migrations still create/update pipeline_writer, worker and agent_ro on the
shared server -- same roles, same passwords from .env.
"""
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from db import ROOT, connect

TEST_DB = "fraud_phase1_test"


@pytest.fixture(scope="session")
def migrate_runs():
    """Create TEST_DB, run the migrator on it twice, drop it afterwards."""
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {TEST_DB}")

    runs = [
        subprocess.run(
            [sys.executable, "-m", "db.migrate", "--dbname", TEST_DB],
            cwd=ROOT, capture_output=True, text=True,
        )
        for _ in range(2)
    ]
    yield runs

    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
    admin.close()


@pytest.fixture
def db(migrate_runs):
    """Admin connection to TEST_DB with the data tables emptied."""
    assert migrate_runs[0].returncode == 0, migrate_runs[0].stderr
    conn = connect("admin", TEST_DB)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE analyst_decisions, investigation_attempts, investigations, anomaly_flags, transactions, "
            "account_profiles, merchant_risk, ground_truth_labels RESTART IDENTITY"
        )
    yield conn
    conn.close()


@pytest.fixture
def as_role(migrate_runs):
    """Open connections to TEST_DB as a named role; closed at teardown."""
    opened = []

    def _open(role):
        conn = connect(role, TEST_DB)
        conn.autocommit = True
        opened.append(conn)
        return conn

    yield _open
    for conn in opened:
        conn.close()


# ---------- data helpers ----------

def now_utc():
    return datetime.now(timezone.utc)


def insert_txn(cur, txn_id, user_id, ts, amount, merchant="Test Merchant", city="Testville"):
    cur.execute(
        """INSERT INTO transactions
               (transaction_id, event_timestamp, user_id, amount, currency, merchant,
                merchant_category, card_type, card_last4, location_city, location_country, is_fraud)
           VALUES (%s, %s, %s, %s, 'USD', %s, 'online_retail', 'visa', '0000', %s, 'XX', true)""",
        (txn_id, ts, user_id, amount, merchant, city),
    )


def insert_flag(cur, user_id, window_start, window_end, txn_count=1, total_amount=500):
    """Same upsert the Spark job runs (spark_fraud_detection.py)."""
    cur.execute(
        """INSERT INTO anomaly_flags (user_id, window_start, window_end, txn_count, total_amount)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (user_id, window_start, window_end)
           DO UPDATE SET txn_count = EXCLUDED.txn_count,
                         total_amount = EXCLUDED.total_amount,
                         detected_at = now()""",
        (user_id, window_start, window_end, txn_count, total_amount),
    )


def burst_windows(event_time):
    """The 4 sliding windows (2 min long, 30 s slide) that contain event_time,
    exactly as Spark emits them for one burst."""
    floor = event_time.replace(second=(event_time.second // 30) * 30, microsecond=0)
    starts = [floor - timedelta(seconds=30 * k) for k in range(4)]
    return [(s, s + timedelta(minutes=2)) for s in sorted(starts)]


def enqueue(cur, settle="5 minutes", grace="30 minutes", since="-infinity"):
    cur.execute(
        "SELECT enqueue_incidents(%s::interval, %s::interval, %s::timestamptz)",
        (settle, grace, since),
    )
    return cur.fetchone()[0]


def expect_denied(conn, sql, params=None):
    """Assert the statement is refused (no privilege or read-only transaction)."""
    with conn.cursor() as cur:
        with pytest.raises((psycopg2.errors.InsufficientPrivilege,
                            psycopg2.errors.ReadOnlySqlTransaction)):
            cur.execute(sql, params)
