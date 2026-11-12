"""
docs/PLAN_HITL.md Phase H1: deterministic risk signals and a baseline second opinion
on every incident. Throwaway database, so it never touches fraud_db.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from db import connect
from db.migrate import migrate, recreate_database
from investigation_worker import Worker, WorkerConfig

H1_DB = "fraud_h1_test"
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)

GOOD = {"risk_score": 80, "verdict": "likely_fraud", "explanation": "Fake.",
        "evidence_cited": ["fake"], "recommended_action": "block_and_notify_customer"}
BASELINE = {"risk_score": 35, "verdict": "suspicious", "explanation": "Rule-based score: fake.",
            "evidence_cited": ["fake rule"], "recommended_action": "hold_for_review"}


@pytest.fixture(scope="module")
def h1_db():
    recreate_database(H1_DB)
    migrate(H1_DB)
    yield H1_DB
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{H1_DB}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(h1_db):
    conn = connect("admin", h1_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions, anomaly_flags, "
                    "account_profiles, merchant_risk RESTART IDENTITY")
    yield conn
    conn.close()


def add_txn(cur, txn_id, user, ts, amount, merchant="Corner Shop", city="Toronto"):
    cur.execute(
        """INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency,
                                     merchant, location_city, location_country, is_fraud)
           VALUES (%s, %s, %s, %s, 'USD', %s, %s, 'CA', false)""",
        (txn_id, ts, user, amount, merchant, city),
    )


def add_flag(cur, user, start, txn_count, total):
    cur.execute(
        """INSERT INTO anomaly_flags (user_id, window_start, window_end, txn_count, total_amount)
           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
        (user, start, start + timedelta(minutes=2), txn_count, total),
    )
    return cur.fetchone()[0]


def add_incident(cur, user, flag_ids, txn_ids, primary):
    cur.execute(
        """INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids, primary_txn_id)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
        (user, T0, T0 + timedelta(minutes=3), flag_ids, txn_ids, primary),
    )
    return cur.fetchone()[0]


def velocity_burst(cur, user="u_burst"):
    """Three $20 purchases in a minute: the count rule fires, the spend rule doesn't."""
    ids = []
    for i in range(3):
        add_txn(cur, f"{user}_t{i}", user, T0 + timedelta(seconds=20 * i), 20)
        ids.append(f"{user}_t{i}")
    flags = [add_flag(cur, user, T0, 3, 60), add_flag(cur, user, T0 - timedelta(seconds=30), 2, 40)]
    return add_incident(cur, user, flags, ids, ids[-1])


def big_purchase(cur, user="u_big"):
    """One $400 purchase: the spend rule fires, the count rule doesn't."""
    add_txn(cur, f"{user}_t0", user, T0 + timedelta(seconds=10), 400, merchant="Gadget Hub", city="Paris")
    flags = [add_flag(cur, user, T0, 1, 400)]
    return add_incident(cur, user, flags, [f"{user}_t0"], f"{user}_t0")


def compute(cur, iid):
    cur.execute("SELECT compute_incident_signals(%s)", (iid,))
    cur.execute("SELECT rule_hits, signals FROM investigations WHERE id = %s", (iid,))
    return cur.fetchone()


# ---------- compute_incident_signals ----------

def test_velocity_only_burst(db):
    with db.cursor() as cur:
        rule_hits, signals = compute(cur, velocity_burst(cur))
    assert rule_hits == ["velocity_gt_2_in_2min"]
    assert signals["max_txn_count"] == 3 and float(signals["max_window_total"]) == 60.0


def test_single_large_purchase(db):
    with db.cursor() as cur:
        rule_hits, _ = compute(cur, big_purchase(cur))
    assert rule_hits == ["spend_gt_300_in_2min"]


def test_both_rules(db):
    with db.cursor() as cur:
        user = "u_both"
        for i in range(3):
            add_txn(cur, f"{user}_t{i}", user, T0 + timedelta(seconds=20 * i), 150)
        iid = add_incident(cur, user, [add_flag(cur, user, T0, 3, 450)],
                           [f"{user}_t{i}" for i in range(3)], f"{user}_t2")
        rule_hits, _ = compute(cur, iid)
    assert rule_hits == ["spend_gt_300_in_2min", "velocity_gt_2_in_2min"]


def test_signals_from_profile_merchant_and_history(db):
    with db.cursor() as cur:
        cur.execute("""INSERT INTO account_profiles (user_id, home_city, account_opened_at, avg_monthly_spend, source)
                       VALUES ('u_big', 'Toronto', %s, 800, 'generator')""", (T0 - timedelta(days=100),))
        cur.execute("INSERT INTO merchant_risk (merchant, risk_level) VALUES ('Gadget Hub', 'high')")
        add_txn(cur, "u_big_old", "u_big", T0 - timedelta(days=3), 50, merchant="Corner Shop")
        _, signals = compute(cur, big_purchase(cur))
    assert signals == {
        "max_txn_count": 1, "max_window_total": 400.0, "primary_amount": 400.0,
        "amount_to_avg_monthly_spend": 0.5, "primary_city_is_home": False,
        "new_merchant_for_account": True, "merchant_risk_level": "high", "account_age_days": 100,
    }


def test_missing_profile_and_merchant_are_unknown_not_risky(db):
    with db.cursor() as cur:
        add_txn(cur, "u_big_old", "u_big", T0 - timedelta(days=3), 50, merchant="Gadget Hub")
        _, signals = compute(cur, big_purchase(cur))
    assert signals["merchant_risk_level"] == "unknown"
    assert signals["amount_to_avg_monthly_spend"] is None
    assert signals["primary_city_is_home"] is None and signals["account_age_days"] is None
    assert signals["new_merchant_for_account"] is False


def test_rerunning_does_not_change_the_row(db):
    with db.cursor() as cur:
        iid = big_purchase(cur)
        first = compute(cur, iid)
        cur.execute("INSERT INTO merchant_risk (merchant, risk_level) VALUES ('Gadget Hub', 'low')")
        add_flag(cur, "u_big", T0 + timedelta(seconds=30), 5, 900)
        assert compute(cur, iid) == first


# ---------- worker ----------

def fake_investigate(seen):
    def investigate(account_id, txn_id, merchant, amount, context=None, verbose=False):
        seen.append(context)
        return {"verdict": dict(GOOD), "trace": [], "usage": {"turns": 1}}
    return investigate


def fake_baseline(account_id, txn_id, merchant, amount, context=None, verbose=False):
    return {"verdict": dict(BASELINE), "trace": []}


def make_worker(seen, baseline=fake_baseline):
    cfg = WorkerConfig(concurrency=1, hourly_budget=10_000, timeout_seconds=5, max_attempts=3,
                       backoff_seconds=0, stuck_after_seconds=600, poll_seconds=0.05)
    return Worker(cfg, dbname=H1_DB, investigate=fake_investigate(seen), baseline=baseline,
                  provider="fake", model="fake-model-1")


def test_worker_fills_signals_and_baseline_on_every_done_row(db):
    with db.cursor() as cur:
        velocity_burst(cur)
        big_purchase(cur)
    make_worker([]).run_until_idle()
    with db.cursor() as cur:
        cur.execute("""SELECT status, rule_hits, signals, baseline_score, baseline_verdict, baseline_action
                       FROM investigations ORDER BY id""")
        rows = cur.fetchall()
    assert [r[0] for r in rows] == ["done", "done"]
    assert [r[1] for r in rows] == [["velocity_gt_2_in_2min"], ["spend_gt_300_in_2min"]]
    assert all(r[2]["primary_amount"] is not None for r in rows)
    assert all((float(r[3]), r[4], r[5]) == (35.0, "suspicious", "hold_for_review") for r in rows)


def test_baseline_is_kept_when_the_agent_fails(db):
    with db.cursor() as cur:
        big_purchase(cur)
    cfg = WorkerConfig(concurrency=1, hourly_budget=10_000, timeout_seconds=5, max_attempts=3,
                       backoff_seconds=0, stuck_after_seconds=600, poll_seconds=0.05)

    def broken(*args, **kwargs):
        raise RuntimeError("tool exploded")

    Worker(cfg, dbname=H1_DB, investigate=broken, baseline=fake_baseline,
           provider="fake", model="fake-model-1").run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT status, baseline_verdict, rule_hits FROM investigations")
        assert cur.fetchone() == ("failed", "suspicious", ["spend_gt_300_in_2min"])


def test_rule_hits_reach_the_agent_under_v4_only(db, monkeypatch):
    with db.cursor() as cur:
        velocity_burst(cur)
    seen = []
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v4")
    make_worker(seen).run_until_idle()
    assert "velocity_gt_2_in_2min" in seen[0]

    with db.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions, anomaly_flags RESTART IDENTITY")
        velocity_burst(cur)
    seen.clear()
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v2")
    make_worker(seen).run_until_idle()
    assert "velocity_gt_2_in_2min" not in seen[0] and "Rules that fired" not in seen[0]


# ---------- access ----------

def test_agent_ro_still_cannot_read_investigations(h1_db):
    conn = connect("agent_ro", h1_db)
    try:
        with conn.cursor() as cur, pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("SELECT rule_hits, signals FROM investigations")
    finally:
        conn.close()


def test_agent_ro_cannot_compute_signals(h1_db):
    conn = connect("agent_ro", h1_db)
    try:
        with conn.cursor() as cur, pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("SELECT compute_incident_signals(1)")
    finally:
        conn.close()
