"""
docs/PLAN_HITL.md Phase H5: the Streamlit review queue.

review_data.py holds every query and the decision check the review pages use; the pages
(pages/review_queue.py, pages/case_detail.py, pages/oversight.py) only lay them out. The
data functions are tested as the analyst role (SET ROLE, no password needed). The plan's
walkthrough (override a held case, and the override-without-reason error) is driven
through the real case page with Streamlit's AppTest, logged in as analyst; that part
needs ANALYST_PASSWORD in .env. Everything runs on a throwaway database.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import os
import re
from datetime import datetime, timedelta, timezone

import pytest

import review_data
from db import ROOT, connect
from db.migrate import migrate, recreate_database

H5_DB = "fraud_h5_test"
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
REVIEW_PAGES = ["pages/review_queue.py", "pages/case_detail.py", "pages/oversight.py"]
needs_login = pytest.mark.skipif(not os.getenv("ANALYST_PASSWORD"),
                                 reason="ANALYST_PASSWORD not set; analyst stays NOLOGIN")


@pytest.fixture(scope="module")
def h5_db():
    recreate_database(H5_DB)
    migrate(H5_DB)
    yield H5_DB
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{H5_DB}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(h5_db):
    conn = connect("admin", h5_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions, "
                    "ground_truth_labels RESTART IDENTITY")
    yield conn
    conn.close()


@pytest.fixture
def analyst(h5_db):
    """Admin connection to H5_DB switched to the analyst role."""
    conn = connect("admin", h5_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET ROLE analyst")
    yield conn
    conn.close()


def add_txn(cur, txn_id, user, ts, amount, merchant="Shop A", city="Paris"):
    cur.execute(
        """INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency, merchant,
                                     merchant_category, card_type, card_last4, location_city,
                                     location_country, is_fraud)
           VALUES (%s, %s, %s, %s, 'USD', %s, 'travel', 'visa', '0000', %s, 'FR', true)""",
        (txn_id, ts, user, amount, merchant, city),
    )


def add_case(cur, user="u1", review_status="needs_review", priority=75, qa_sample=False,
             verdict="suspicious", risk_score=55, action="hold_for_review", txns=(), status="done",
             finished_at=T0, evidence=None, trace=None):
    """A finished, routed investigation. txns: (txn_id, amount) pairs; the largest is primary."""
    for i, (txn_id, amount) in enumerate(txns):
        add_txn(cur, txn_id, user, T0 + timedelta(seconds=10 * i), amount)
    primary = max(txns, key=lambda t: t[1])[0] if txns else None
    cur.execute(
        """INSERT INTO investigations
               (user_id, incident_start, incident_end, flag_ids, txn_ids, primary_txn_id, status,
                risk_score, verdict, recommended_action, explanation, evidence, trace,
                rule_hits, signals, baseline_score, baseline_verdict, baseline_action,
                review_status, review_reasons, review_priority, qa_sample, finished_at)
           VALUES (%s, %s, %s, '{}', %s, %s, %s, %s, %s, %s, 'Amount is high for this account.',
                   %s::jsonb, %s::jsonb, '{spend_gt_300_in_2min}',
                   '{"primary_amount": 900, "account_age_days": 400}', 20, 'legitimate', 'clear',
                   %s, '{agent_baseline_disagree}', %s, %s, %s)
           RETURNING id""",
        (user, T0, T0 + timedelta(minutes=2), [t[0] for t in txns], primary, status, risk_score,
         verdict, action, evidence or '["amount above average"]', trace or "[]",
         review_status, priority, qa_sample, finished_at),
    )
    return cur.fetchone()[0]


def decision_rows(db, iid):
    with db.cursor() as cur:
        cur.execute("SELECT analyst, decision, final_action, reason FROM analyst_decisions "
                    "WHERE investigation_id = %s ORDER BY id", (iid,))
        return cur.fetchall()


def status_of(db, iid):
    with db.cursor() as cur:
        cur.execute("SELECT review_status FROM investigations WHERE id = %s", (iid,))
        return cur.fetchone()[0]


# ---------- 1. pages connect as analyst, never fraud_user ----------

def test_review_pages_exist_and_never_use_admin_credentials():
    for page in REVIEW_PAGES:
        text = (ROOT / page).read_text(encoding="utf-8")
        assert "fraud_user" not in text and "fraud_pass" not in text, page
        assert not re.search(r"connect(_kwargs)?\(\s*['\"]admin", text), page
        assert "review_data" in text, page


def test_review_data_connects_as_analyst_via_connect_kwargs(monkeypatch):
    seen = {}

    def fake_connect(**kwargs):
        seen.update(kwargs)
        raise RuntimeError("stop")

    monkeypatch.setenv("ANALYST_PASSWORD", "x")
    monkeypatch.setattr(review_data.psycopg2, "connect", fake_connect)
    with pytest.raises(RuntimeError, match="stop"):
        review_data.connect()
    assert seen["user"] == "analyst"


def test_dashboard_is_multipage_with_pipeline_first():
    text = (ROOT / "streamlit_dashboard.py").read_text(encoding="utf-8")
    assert "st.navigation" in text
    pages = re.findall(r'st\.Page\(\s*"([^"]+)"', text)
    assert pages[0] == "pages/pipeline.py"
    assert set(REVIEW_PAGES) <= set(pages)
    assert (ROOT / "pages/pipeline.py").exists()


# ---------- 2. review queue ----------

def test_queue_holds_open_cases_sorted_by_priority(db, analyst):
    with db.cursor() as cur:
        low = add_case(cur, "u_low", "needs_review", priority=60)
        held = add_case(cur, "u_held", "held_pending_review", priority=140)
        qa = add_case(cur, "u_qa", "auto_cleared", priority=0, qa_sample=True, verdict="legitimate")
        add_case(cur, "u_auto", "auto_cleared", priority=0, verdict="legitimate")
        add_case(cur, "u_done", "reviewed", priority=200)
        add_case(cur, "u_unrouted", None, priority=None)
    queue = review_data.queue_cases(analyst)
    assert list(queue["id"]) == [held, low, qa]
    assert list(queue["qa_sample"]) == [False, False, True]


def test_queue_has_the_planned_columns(db, analyst):
    with db.cursor() as cur:
        add_case(cur, "u1", txns=[("t1", 120.0), ("t2", 900.0)], finished_at=datetime.now(timezone.utc)
                 - timedelta(hours=5))
    row = review_data.queue_cases(analyst).iloc[0]
    for col in ["user_id", "amount", "verdict", "risk_score", "baseline_verdict", "baseline_score",
                "review_reasons", "age", "review_status", "review_priority"]:
        assert col in row.index, col
    assert row["amount"] == 900.0
    assert row["user_id"] == "u1" and row["baseline_verdict"] == "legitimate"
    assert list(row["review_reasons"]) == ["agent_baseline_disagree"]
    assert timedelta(hours=4, minutes=59) < row["age"] < timedelta(hours=5, minutes=5)


def test_request_info_keeps_the_case_in_the_queue_and_shows_it(db, analyst):
    with db.cursor() as cur:
        iid = add_case(cur)
    review_data.submit_decision(analyst, iid, "bob", "request_info", None, "need the receipt")
    queue = review_data.queue_cases(analyst)
    assert list(queue["id"]) == [iid]
    assert queue.iloc[0]["last_decision"] == "request_info"


def test_format_age():
    assert review_data.format_age(timedelta(minutes=7)) == "7m"
    assert review_data.format_age(timedelta(hours=5, minutes=3)) == "5h 3m"
    assert review_data.format_age(timedelta(days=18, hours=2)) == "18d 2h"
    assert review_data.format_age(None) == ""


# ---------- 3. case detail ----------

def test_case_detail_has_everything_the_analyst_needs(db, analyst):
    evidence = '[{"claim": "spends 3x usual", "tool": "get_account_history"}, "old string claim"]'
    trace = '[{"step": 0, "type": "tool_call", "tool": "get_account_history", "input": {}, "output": {}}]'
    with db.cursor() as cur:
        iid = add_case(cur, "u1", "held_pending_review", txns=[("t1", 120.0), ("t2", 900.0)],
                       evidence=evidence, trace=trace)
        add_txn(cur, "other_user_txn", "u2", T0, 50.0)
        add_txn(cur, "u1_outside_incident", "u1", T0 + timedelta(days=1), 60.0)
    review_data.submit_decision(analyst, iid, "bob", "request_info", None, "ask the customer")

    case = review_data.case_detail(analyst, iid)
    assert case["case"]["user_id"] == "u1" and case["case"]["review_status"] == "held_pending_review"
    assert list(case["transactions"]["transaction_id"]) == ["t1", "t2"]
    assert "is_fraud" not in case["transactions"].columns
    assert list(case["case"]["rule_hits"]) == ["spend_gt_300_in_2min"]
    assert case["case"]["signals"]["primary_amount"] == 900
    assert case["evidence"] == [{"claim": "spends 3x usual", "tool": "get_account_history"},
                                {"claim": "old string claim", "tool": None}]
    assert case["trace"][0]["tool"] == "get_account_history"
    assert case["case"]["baseline_verdict"] == "legitimate" and case["case"]["baseline_score"] == 20
    assert list(case["decisions"]["decision"]) == ["request_info"]
    assert case["decisions"].iloc[0]["reason"] == "ask the customer"


def test_case_detail_unknown_id_is_none(db, analyst):
    assert review_data.case_detail(analyst, 999) is None


# ---------- 4. the decision check, before anything is written ----------

@pytest.mark.parametrize("decision, action, reason, analyst_name, fragment", [
    ("override", "approve", None, "bob", "reason"),
    ("override", "approve", "   ", "bob", "reason"),
    ("agree", None, None, "bob", "final action"),
    ("override", None, "wrong", "bob", "final action"),
    ("agree", "hold", None, "  ", "name"),
    ("agree", "hold", "x" * 2001, "bob", "2000"),
    ("maybe", "hold", None, "bob", "decision"),
    ("agree", "freeze", None, "bob", "final action"),
])
def test_validate_decision_rejects(decision, action, reason, analyst_name, fragment):
    errors = review_data.validate_decision(decision, action, reason, analyst_name)
    assert errors and any(fragment in e.lower() for e in errors), errors


@pytest.mark.parametrize("decision, action, reason", [
    ("agree", "hold", None),
    ("override", "approve", "normal travel for this customer"),
    ("request_info", None, None),
    ("request_info", "request_otp", "call the customer"),
])
def test_validate_decision_accepts(decision, action, reason):
    assert review_data.validate_decision(decision, action, reason, "bob") == []


def test_submit_override_without_reason_writes_nothing(db, analyst):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review")
    with pytest.raises(review_data.DecisionError):
        review_data.submit_decision(analyst, iid, "bob", "override", "approve", "  ")
    assert decision_rows(db, iid) == []
    assert status_of(db, iid) == "held_pending_review"


def test_submit_override_records_and_closes(db, analyst):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review")
    decision_id = review_data.submit_decision(analyst, iid, " bob ", "override", "approve", "known traveller")
    assert decision_id > 0
    assert decision_rows(db, iid) == [("bob", "override", "approve", "known traveller")]
    assert status_of(db, iid) == "reviewed"
    assert iid not in set(review_data.queue_cases(analyst)["id"])


def test_submit_on_a_case_that_cannot_be_decided_raises_decision_error(db, analyst):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="auto_cleared", verdict="legitimate")
    with pytest.raises(review_data.DecisionError, match="not open for review"):
        review_data.submit_decision(analyst, iid, "bob", "agree", "approve", None)
    assert decision_rows(db, iid) == []


# ---------- 5. oversight ----------

def test_oversight_numbers_on_seeded_decisions(db, analyst):
    with db.cursor() as cur:
        a = add_case(cur, "a", "held_pending_review")
        b = add_case(cur, "b", "needs_review")
        c = add_case(cur, "c", "needs_review")
        add_case(cur, "d", "needs_review")                       # still open
        add_case(cur, "e", "auto_cleared", qa_sample=True, verdict="legitimate")   # open QA sample
        add_case(cur, "f", "auto_cleared", verdict="legitimate")
    review_data.submit_decision(analyst, a, "bob", "override", "approve", "fine")
    review_data.submit_decision(analyst, b, "bob", "agree", "hold", None)
    review_data.submit_decision(analyst, c, "bob", "request_info", None, "?")
    review_data.submit_decision(analyst, c, "bob", "agree", "block", None)
    with db.cursor() as cur:                                    # make decision times known
        cur.execute("ALTER TABLE analyst_decisions DISABLE TRIGGER analyst_decisions_append_only")
        cur.execute("UPDATE analyst_decisions SET decided_at = %s + interval '1 hour' * id", (T0,))
        cur.execute("ALTER TABLE analyst_decisions ENABLE TRIGGER analyst_decisions_append_only")
    # closing decisions: a at +1h, b at +2h, c at +4h (its request_info at +3h doesn't close it)

    o = review_data.oversight(analyst)
    assert o["by_status"] == {"reviewed": 3, "needs_review": 1, "auto_cleared": 2}
    assert o["queue_depth"] == 2                                  # d and the QA sample e
    assert o["decided_cases"] == 3
    assert o["median_time_to_decision"] == timedelta(hours=2)
    assert o["override_rate"] == pytest.approx(1 / 3)


def test_oversight_with_no_decisions(db, analyst):
    o = review_data.oversight(analyst)
    assert o["queue_depth"] == 0 and o["decided_cases"] == 0
    assert o["median_time_to_decision"] is None and o["override_rate"] is None


# ---------- 6. the plan's walkthrough, through the real page ----------

@pytest.fixture
def case_page(db, monkeypatch):
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv("PGDATABASE", H5_DB)

    def _open(case_id):
        at = AppTest.from_file(str(ROOT / "pages/case_detail.py"), default_timeout=30)
        at.session_state["case_id"] = case_id
        return at.run()
    return _open


def fill_and_submit(at, decision, final_action, reason):
    at.text_input(key="analyst_name").set_value("vib")
    at.radio(key="decision").set_value(decision)
    at.selectbox(key="final_action").set_value(final_action)
    at.text_area(key="reason").set_value(reason)
    return at.button(key="FormSubmitter:decision_form-Submit decision").click().run()


@needs_login
def test_walkthrough_override_without_reason_shows_inline_error(db, case_page):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review", txns=[("t1", 900.0)])
    at = case_page(iid)
    assert not at.exception
    at = fill_and_submit(at, "override", "approve", "")
    assert not at.exception
    assert any("reason" in e.value.lower() for e in at.error)
    assert decision_rows(db, iid) == []
    assert status_of(db, iid) == "held_pending_review"


@needs_login
def test_walkthrough_override_held_case_leaves_queue(db, case_page, analyst):
    with db.cursor() as cur:
        iid = add_case(cur, review_status="held_pending_review", txns=[("t1", 900.0)])
    at = case_page(iid)
    assert not at.exception
    assert iid in set(review_data.queue_cases(analyst)["id"])
    at = fill_and_submit(at, "override", "approve", "customer confirmed the trip")
    assert not at.exception
    assert not at.error
    assert decision_rows(db, iid) == [("vib", "override", "approve", "customer confirmed the trip")]
    assert status_of(db, iid) == "reviewed"
    assert iid not in set(review_data.queue_cases(analyst)["id"])
    assert any("recorded" in s.value.lower() for s in at.success)


@needs_login
@pytest.mark.parametrize("page", ["pages/review_queue.py", "pages/oversight.py"])
def test_other_review_pages_render(db, monkeypatch, page):
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv("PGDATABASE", H5_DB)
    with db.cursor() as cur:
        add_case(cur, review_status="held_pending_review", txns=[("t1", 900.0)])
    at = AppTest.from_file(str(ROOT / page), default_timeout=30).run()
    assert not at.exception
