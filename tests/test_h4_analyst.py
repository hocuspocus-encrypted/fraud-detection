"""
docs/PLAN_HITL.md Phase H4: analyst decisions and the audit trail (database side).
Migration 0013 adds the analyst role, the append-only analyst_decisions table, the
record_decision() function (the only path to review_status = 'reviewed') and the
v_case_latest_decision view. Everything runs on a throwaway database.

Role checks use SET ROLE from the admin connection, so they run whether or not
ANALYST_PASSWORD is set in .env (privileges, not login, are what is tested).

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import os
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

import db as db_module
from db import connect
from db.migrate import migrate, recreate_database

H4_DB = "fraud_h4_test"
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
DENIED = (psycopg2.errors.InsufficientPrivilege, psycopg2.errors.RaiseException)


@pytest.fixture(scope="module")
def h4_db():
    recreate_database(H4_DB)
    migrate(H4_DB)
    yield H4_DB
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{H4_DB}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(h4_db):
    conn = connect("admin", h4_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions, "
                    "ground_truth_labels RESTART IDENTITY")
    yield conn
    conn.close()


@pytest.fixture
def as_role(h4_db):
    """Admin connections to H4_DB switched to a role with SET ROLE; closed at teardown."""
    opened = []

    def _open(role):
        conn = connect("admin", h4_db)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET ROLE {role}")
        opened.append(conn)
        return conn

    yield _open
    for conn in opened:
        conn.close()


def add_case(cur, user="u1", review_status="needs_review", qa_sample=False, verdict="suspicious"):
    """A finished, routed investigation (no transactions needed for the decision path)."""
    cur.execute(
        """INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids, status,
                                       risk_score, verdict, recommended_action, review_status,
                                       review_reasons, review_priority, qa_sample, finished_at)
           VALUES (%s, %s, %s, '{}', '{}', 'done', 55, %s, 'hold_for_review', %s, '{uncertain_score}',
                   75, %s, %s) RETURNING id""",
        (user, T0, T0 + timedelta(minutes=2), verdict, review_status, qa_sample, T0),
    )
    return cur.fetchone()[0]


def decide(conn, iid, decision="agree", final_action="hold", reason=None, analyst="alice"):
    with conn.cursor() as cur:
        cur.execute("SELECT record_decision(%s, %s, %s, %s, %s)", (iid, analyst, decision, final_action, reason))
        return cur.fetchone()[0]


def review_status(db, iid):
    with db.cursor() as cur:
        cur.execute("SELECT review_status FROM investigations WHERE id = %s", (iid,))
        return cur.fetchone()[0]


def decision_rows(db, iid=None):
    with db.cursor() as cur:
        cur.execute("SELECT investigation_id, analyst, decision, final_action, reason, agent_verdict_at_decision "
                    "FROM analyst_decisions WHERE investigation_id = coalesce(%s, investigation_id) ORDER BY id",
                    (iid,))
        return cur.fetchall()


def run_denied(conn, sql, params=None):
    with conn.cursor() as cur, pytest.raises(DENIED):
        cur.execute(sql, params)


# ---------- 1. migration and role ----------

def test_migration_is_safe_to_rerun(h4_db):
    assert migrate(h4_db) == 0


def test_analyst_role_exists_and_is_not_superuser(db):
    with db.cursor() as cur:
        cur.execute("SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles WHERE rolname = 'analyst'")
        assert cur.fetchone() == (False, False, False)


def test_analyst_password_comes_from_env(monkeypatch):
    assert db_module.ROLE_PASSWORD_VARS["analyst"] == "ANALYST_PASSWORD"
    monkeypatch.delenv("ANALYST_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="ANALYST_PASSWORD"):
        db_module.connect_kwargs("analyst")
    monkeypatch.setenv("ANALYST_PASSWORD", "s3cret")
    assert db_module.connect_kwargs("analyst")["user"] == "analyst"


@pytest.mark.skipif(not os.getenv("ANALYST_PASSWORD"), reason="ANALYST_PASSWORD not set; analyst stays NOLOGIN")
def test_analyst_can_log_in_when_password_set(h4_db):
    conn = connect("analyst", h4_db)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_user")
            assert cur.fetchone()[0] == "analyst"
    finally:
        conn.close()


# ---------- 2-5. privileges ----------

@pytest.mark.parametrize("relation", ["investigations", "investigation_attempts", "v_agent_transactions",
                                      "analyst_decisions", "v_case_latest_decision"])
def test_analyst_can_read(db, as_role, relation):
    with as_role("analyst").cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {relation}")
        assert cur.fetchone()[0] >= 0


@pytest.mark.parametrize("relation", ["ground_truth_labels", "transactions"])
def test_analyst_cannot_read_ground_truth_or_base_transactions(db, as_role, relation):
    run_denied(as_role("analyst"), f"SELECT 1 FROM {relation} LIMIT 1")


def test_analyst_cannot_update_delete_or_truncate_decisions(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur)
    analyst = as_role("analyst")
    decide(analyst, iid)
    run_denied(analyst, "UPDATE analyst_decisions SET reason = 'edited'")
    run_denied(analyst, "DELETE FROM analyst_decisions")
    run_denied(analyst, "TRUNCATE analyst_decisions")
    assert len(decision_rows(db)) == 1 and decision_rows(db)[0][4] is None


def test_decisions_are_append_only_even_for_the_owner(db, as_role):
    """A trigger blocks UPDATE and DELETE for every role, as an audit trail should."""
    with db.cursor() as cur:
        iid = add_case(cur)
    decide(as_role("analyst"), iid)
    run_denied(db, "UPDATE analyst_decisions SET reason = 'edited'")
    run_denied(db, "DELETE FROM analyst_decisions")


def test_analyst_writes_only_through_record_decision(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur)
    analyst = as_role("analyst")
    run_denied(analyst, "INSERT INTO analyst_decisions (investigation_id, analyst, decision, final_action) "
                        "VALUES (%s, 'alice', 'agree', 'hold')", (iid,))
    run_denied(analyst, "UPDATE investigations SET review_status = 'reviewed' WHERE id = %s", (iid,))
    assert review_status(db, iid) == "needs_review" and decision_rows(db) == []


@pytest.mark.parametrize("role", ["agent_ro", "worker"])
def test_agent_and_worker_cannot_see_decisions(db, as_role, role):
    with db.cursor() as cur:
        iid = add_case(cur)
    conn = as_role(role)
    run_denied(conn, "SELECT 1 FROM analyst_decisions LIMIT 1")
    run_denied(conn, "SELECT 1 FROM v_case_latest_decision LIMIT 1")
    run_denied(conn, "SELECT record_decision(%s, 'x', 'agree', 'approve', NULL)", (iid,))
    assert decision_rows(db) == []


def test_record_decision_is_security_definer_and_not_public(db):
    with db.cursor() as cur:
        cur.execute("SELECT prosecdef, proconfig FROM pg_proc WHERE proname = 'record_decision'")
        secdef, config = cur.fetchone()
        assert secdef and any(c.startswith("search_path=") for c in config)
        cur.execute("SELECT has_function_privilege('public', 'record_decision(bigint, text, text, text, text)', "
                    "'EXECUTE')")
        assert cur.fetchone()[0] is False


# ---------- 6. override needs a reason ----------

@pytest.mark.parametrize("reason", [None, "", "   "])
def test_override_without_reason_is_rejected(db, as_role, reason):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review")
    with pytest.raises(psycopg2.errors.CheckViolation):
        decide(as_role("analyst"), iid, decision="override", final_action="approve", reason=reason)
    assert decision_rows(db) == [] and review_status(db, iid) == "held_pending_review"


def test_override_reason_is_also_enforced_on_the_table(db):
    with db.cursor() as cur:
        iid = add_case(cur)
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute("INSERT INTO analyst_decisions (investigation_id, analyst, decision, final_action) "
                        "VALUES (%s, 'alice', 'override', 'approve')", (iid,))


@pytest.mark.parametrize("decision, final_action", [("approve", "approve"), ("agree", "release"), ("agree", None)])
def test_unknown_decision_or_action_is_rejected(db, as_role, decision, final_action):
    with db.cursor() as cur:
        iid = add_case(cur)
    with pytest.raises(psycopg2.errors.CheckViolation):
        decide(as_role("analyst"), iid, decision=decision, final_action=final_action)
    assert decision_rows(db) == []


def test_blank_analyst_name_is_rejected(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur)
    with pytest.raises(psycopg2.errors.CheckViolation):
        decide(as_role("analyst"), iid, analyst="  ")


def test_reason_length_is_capped(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur)
    with pytest.raises(psycopg2.errors.CheckViolation):
        decide(as_role("analyst"), iid, decision="override", final_action="approve", reason="x" * 2001)


# ---------- 7. which cases can be decided ----------

@pytest.mark.parametrize("status, qa", [("needs_review", False), ("held_pending_review", False),
                                        ("auto_cleared", True)])
def test_queue_cases_accept_a_decision(db, as_role, status, qa):
    with db.cursor() as cur:
        iid = add_case(cur, review_status=status, qa_sample=qa, verdict="legitimate")
    decide(as_role("analyst"), iid, final_action="approve")
    assert review_status(db, iid) == "reviewed"


@pytest.mark.parametrize("status, qa", [(None, False), ("auto_cleared", False)])
def test_cases_outside_the_queue_are_rejected(db, as_role, status, qa):
    with db.cursor() as cur:
        iid = add_case(cur, review_status=status, qa_sample=qa)
    with pytest.raises(psycopg2.errors.ObjectNotInPrerequisiteState):
        decide(as_role("analyst"), iid)
    assert decision_rows(db) == [] and review_status(db, iid) == status


def test_unknown_investigation_is_rejected(db, as_role):
    with pytest.raises(psycopg2.errors.NoDataFound):
        decide(as_role("analyst"), 999)


# ---------- 8. what a decision records ----------

def test_agree_records_the_decision_and_marks_reviewed(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur, verdict="likely_fraud")
    did = decide(as_role("analyst"), iid, decision="agree", final_action="block", analyst=" alice ")
    assert decision_rows(db) == [(iid, "alice", "agree", "block", None, "likely_fraud")]
    assert review_status(db, iid) == "reviewed"
    with db.cursor() as cur:
        cur.execute("SELECT decided_at FROM analyst_decisions WHERE id = %s", (did,))
        assert cur.fetchone()[0] is not None


def test_override_with_reason_is_recorded(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review")
    decide(as_role("analyst"), iid, decision="override", final_action="approve",
           reason="Customer confirmed the trip by phone")
    assert decision_rows(db) == [(iid, "alice", "override", "approve", "Customer confirmed the trip by phone",
                                  "suspicious")]
    assert review_status(db, iid) == "reviewed"


def test_request_info_keeps_the_case_in_the_queue(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review")
    decide(as_role("analyst"), iid, decision="request_info", final_action="request_otp")
    decide(as_role("analyst"), iid, decision="request_info", final_action=None)
    assert [row[2] for row in decision_rows(db)] == ["request_info", "request_info"]
    assert review_status(db, iid) == "held_pending_review"


def test_deleting_a_decided_investigation_is_refused(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur)
    decide(as_role("analyst"), iid)
    run_denied_fk = pytest.raises(psycopg2.errors.ForeignKeyViolation)
    with db.cursor() as cur, run_denied_fk:
        cur.execute("DELETE FROM investigations WHERE id = %s", (iid,))


# ---------- 9. two decisions, latest wins ----------

def test_two_decisions_keep_both_rows_and_latest_wins(db, as_role):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review")
        other = add_case(cur, user="u2")
    analyst = as_role("analyst")
    decide(analyst, iid, decision="agree", final_action="hold", analyst="alice")
    decide(analyst, iid, decision="override", final_action="approve", reason="Second look: known merchant",
           analyst="bob")
    decide(analyst, other, decision="agree", final_action="approve")

    assert [(row[1], row[2]) for row in decision_rows(db, iid)] == [("alice", "agree"), ("bob", "override")]
    with analyst.cursor() as cur:
        cur.execute("SELECT investigation_id, analyst, decision, final_action, decision_count "
                    "FROM v_case_latest_decision ORDER BY investigation_id")
        assert cur.fetchall() == [(iid, "bob", "override", "approve", 2), (other, "alice", "agree", "approve", 1)]
    assert review_status(db, iid) == "reviewed"
