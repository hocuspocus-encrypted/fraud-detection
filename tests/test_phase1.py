"""
Phase 1 acceptance checks (PLAN.md, Phase 1). Requires the compose stack:

    docker compose up -d
    venv\\Scripts\\python.exe -m pytest tests -q
"""
from datetime import timedelta

import psycopg2
import pytest

from conftest import (TEST_DB, burst_windows, enqueue, expect_denied, insert_flag,
                      insert_txn, now_utc)
from db.migrate import MIGRATIONS_DIR


# ---------- migrations ----------

def test_migrations_apply_twice(migrate_runs):
    first, second = migrate_runs
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert "applied 0 migration(s)" in second.stdout


def test_each_migration_file_is_rerunnable(db):
    """Even bypassing schema_migrations, every file can be executed again."""
    with db.cursor() as cur:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            cur.execute(path.read_text(encoding="utf-8"))


# ---------- enqueue (D2) ----------

def test_overlapping_flags_become_one_incident(db):
    t = now_utc() - timedelta(hours=1)
    with db.cursor() as cur:
        insert_txn(cur, "a1", "u1", t, 100)
        insert_txn(cur, "a2", "u1", t + timedelta(seconds=10), 400)
        insert_txn(cur, "a3", "u1", t + timedelta(seconds=20), 400)  # ties a2 on amount, later wins
        for ws, we in burst_windows(t):
            insert_flag(cur, "u1", ws, we, txn_count=3, total_amount=900)

        assert enqueue(cur) == 1
        cur.execute("SELECT status, flag_ids, txn_ids, primary_txn_id FROM investigations")
        rows = cur.fetchall()
        assert len(rows) == 1
        status, flag_ids, txn_ids, primary = rows[0]
        assert status == "pending"
        assert len(flag_ids) == 4
        assert txn_ids == ["a1", "a2", "a3"]
        assert primary == "a3"

        # polling again, re-upserting a window, or a late overlapping window: no new work
        assert enqueue(cur) == 0
        ws, we = burst_windows(t)[0]
        insert_flag(cur, "u1", ws, we, txn_count=4, total_amount=1200)
        insert_flag(cur, "u1", we - timedelta(seconds=30), we + timedelta(seconds=90))
        assert enqueue(cur) == 0
        cur.execute("SELECT count(*) FROM investigations")
        assert cur.fetchone()[0] == 1


def test_repeat_polls_do_not_burn_ids(db):
    """ON CONFLICT DO NOTHING still draws a sequence value per attempted row, so
    re-offering existing incidents every poll would burn ~N ids per tick."""
    t = now_utc() - timedelta(hours=1)
    with db.cursor() as cur:
        insert_txn(cur, "r1", "u12", t, 500)
        for ws, we in burst_windows(t):
            insert_flag(cur, "u12", ws, we)
        assert enqueue(cur) == 1
        cur.execute("SELECT last_value FROM investigations_id_seq")
        before = cur.fetchone()[0]
        for _ in range(5):
            assert enqueue(cur) == 0
        cur.execute("SELECT last_value FROM investigations_id_seq")
        assert cur.fetchone()[0] == before


def test_unsettled_incident_waits(db):
    t = now_utc() - timedelta(minutes=4)  # last window ends ~2 min ago, settle is 5 min
    with db.cursor() as cur:
        insert_txn(cur, "b1", "u2", t, 500)
        for ws, we in burst_windows(t):
            insert_flag(cur, "u2", ws, we)
        assert enqueue(cur) == 0
        assert enqueue(cur, settle="1 minute") == 1


def test_separate_bursts_become_two_incidents(db):
    t1 = now_utc() - timedelta(hours=2)
    t2 = now_utc() - timedelta(hours=1)
    with db.cursor() as cur:
        for i, t in enumerate((t1, t2)):
            insert_txn(cur, f"c{i}", "u3", t, 500)
            for ws, we in burst_windows(t):
                insert_flag(cur, "u3", ws, we)
        assert enqueue(cur) == 2
        cur.execute("SELECT primary_txn_id FROM investigations ORDER BY incident_start")
        assert [r[0] for r in cur.fetchall()] == ["c0", "c1"]


def test_incident_without_transactions_fails_after_grace(db):
    with db.cursor() as cur:
        recent = now_utc() - timedelta(minutes=12)  # settled, still inside the 30 min grace
        for ws, we in burst_windows(recent):
            insert_flag(cur, "u4", ws, we)
        assert enqueue(cur) == 0

        old = now_utc() - timedelta(hours=1)
        for ws, we in burst_windows(old):
            insert_flag(cur, "u5", ws, we)
        assert enqueue(cur) == 1
        cur.execute("SELECT user_id, status, error, primary_txn_id, finished_at FROM investigations")
        user_id, status, error, primary, finished_at = cur.fetchone()
        assert (user_id, status, primary) == ("u5", "failed", None)
        assert "no transactions" in error
        assert finished_at is not None


def test_flags_before_since_are_ignored(db):
    t = now_utc() - timedelta(days=2)
    with db.cursor() as cur:
        insert_txn(cur, "d1", "u6", t, 500)
        for ws, we in burst_windows(t):
            insert_flag(cur, "u6", ws, we)
        assert enqueue(cur, since=(now_utc() - timedelta(days=1)).isoformat()) == 0
        assert enqueue(cur) == 1


# ---------- agent view and profiles ----------

def test_agent_view_hides_label_and_caps_text(db):
    with db.cursor() as cur:
        insert_txn(cur, "e1", "u7", now_utc(), 10, merchant="M" * 255, city="C" * 128)
        cur.execute("SELECT * FROM v_agent_transactions")
        cols = [d.name for d in cur.description]
        row = dict(zip(cols, cur.fetchone()))
    assert "is_fraud" not in cols
    assert "card_last4" not in cols
    assert len(row["merchant"]) <= 100
    assert len(row["location_city"]) <= 100


def test_refresh_account_profiles(db):
    t = now_utc() - timedelta(days=60)
    with db.cursor() as cur:
        insert_txn(cur, "f1", "u8", t, 100, city="Muscat")
        insert_txn(cur, "f2", "u8", t + timedelta(days=10), 100, city="Muscat")
        insert_txn(cur, "f3", "u8", t + timedelta(days=59), 100, city="Dubai")
        cur.execute(
            "INSERT INTO account_profiles (user_id, home_city, account_opened_at, avg_monthly_spend, source) "
            "VALUES ('u9', 'Lagos', now() - interval '3 years', 250, 'generator')"
        )
        insert_txn(cur, "f4", "u9", t, 999, city="Elsewhere")

        cur.execute("SELECT refresh_account_profiles()")
        cur.execute("SELECT user_id, home_city, account_opened_at, avg_monthly_spend, source "
                    "FROM account_profiles ORDER BY user_id")
        u8, u9 = cur.fetchall()
    assert u8[1] == "Muscat"
    assert u8[2] == t
    assert float(u8[3]) == pytest.approx(300 / (59 / 30), rel=0.01)
    assert u8[4] == "derived"
    assert u9[1:2] == ("Lagos",) and u9[4] == "generator"  # generator profiles are never overwritten


# ---------- roles ----------

ALL_TABLES_SQL = """
    SELECT format('%I.%I', table_schema, table_name) FROM information_schema.tables
    WHERE table_schema = 'public'
"""


def test_agent_ro_has_no_write_privilege_anywhere(db):
    with db.cursor() as cur:
        cur.execute(ALL_TABLES_SQL)
        names = [r[0] for r in cur.fetchall()]
        assert names
        for name in names:
            cur.execute(
                "SELECT has_table_privilege('agent_ro', %s, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')",
                (name,),
            )
            assert cur.fetchone()[0] is False, name
        cur.execute("SELECT has_schema_privilege('agent_ro', 'public', 'CREATE')")
        assert cur.fetchone()[0] is False


def test_agent_ro_reads_only_the_allow_list(db, as_role):
    agent = as_role("agent_ro")
    with agent.cursor() as cur:
        for allowed in ("v_agent_transactions", "account_profiles", "merchant_risk",
                        "account_similarity", "fraud_clusters", "fraud_cluster_members"):
            cur.execute(f"SELECT count(*) FROM {allowed}")
    for denied in ("transactions", "ground_truth_labels", "investigations", "anomaly_flags",
                   "schema_migrations"):
        expect_denied(agent, f"SELECT 1 FROM {denied} LIMIT 1")
    expect_denied(agent, "SELECT enqueue_incidents()")
    expect_denied(agent, "SELECT refresh_account_profiles()")


def test_agent_ro_cannot_write_even_with_read_only_off(db, as_role):
    agent = as_role("agent_ro")
    with agent.cursor() as cur:
        cur.execute("SHOW default_transaction_read_only")
        assert cur.fetchone()[0] == "on"
        cur.execute("SET default_transaction_read_only = off")
    expect_denied(agent, "INSERT INTO merchant_risk (merchant, risk_level, reason) VALUES ('x', 'low', 'x')")
    expect_denied(agent, "UPDATE account_profiles SET home_city = 'x'")
    expect_denied(agent, "CREATE TABLE agent_scratch (id int)")


def test_worker_runs_queue_but_cannot_see_ground_truth(db, as_role):
    t = now_utc() - timedelta(hours=1)
    with db.cursor() as cur:
        insert_txn(cur, "g1", "u10", t, 500)
        for ws, we in burst_windows(t):
            insert_flag(cur, "u10", ws, we)
    worker = as_role("worker")
    with worker.cursor() as cur:
        assert enqueue(cur) == 1
        cur.execute("UPDATE investigations SET status = 'running', claimed_at = now() WHERE user_id = 'u10'")
        assert cur.rowcount == 1
    expect_denied(worker, "SELECT 1 FROM ground_truth_labels")
    expect_denied(worker, "DELETE FROM transactions")
    expect_denied(worker, "INSERT INTO anomaly_flags (user_id, window_start, window_end, txn_count, total_amount) "
                          "VALUES ('x', now(), now(), 1, 1)")


def test_pipeline_writer_can_write_pipeline_tables(db, as_role):
    writer = as_role("pipeline_writer")
    with writer.cursor() as cur:
        insert_txn(cur, "h1", "u11", now_utc(), 5)
        insert_flag(cur, "u11", now_utc(), now_utc() + timedelta(minutes=2))
        cur.execute("INSERT INTO ground_truth_labels (label_type, user_id) VALUES ('test', 'u11')")
    expect_denied(writer, "SELECT 1 FROM investigations")


def test_roles_can_log_in_to_test_db_only_with_password(migrate_runs):
    from db import connect as _connect
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(host="localhost", dbname=TEST_DB, user="agent_ro", password="wrong")
    _connect("agent_ro", TEST_DB).close()
