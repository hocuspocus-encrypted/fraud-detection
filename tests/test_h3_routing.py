"""
docs/PLAN_HITL.md Phase H3: deterministic routing. routing.route() is a pure function;
the worker writes its result in the same transaction as the investigation result, and
route_existing.py backfills older rows. The database checks use a throwaway database.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import itertools
import json
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

import route_existing
import routing
from db import ROOT, connect
from db.migrate import migrate, recreate_database
from investigation_worker import Worker, WorkerConfig
from routing import RoutingConfig, route

sys.path.insert(0, str(ROOT / "agent"))
import tools  # noqa: E402

H3_DB = "fraud_h3_test"
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
CFG = RoutingConfig()  # the defaults: $2500, 30-70, 10%, 30 days

LEGIT = {"risk_score": 10, "verdict": "legitimate", "recommended_action": "approve", "confidence": "high"}
BASE_LEGIT = {"risk_score": 5, "verdict": "legitimate", "recommended_action": "approve"}
SIGNALS = {"primary_amount": 45.0, "account_age_days": 400}


def r(agent=LEGIT, signals=SIGNALS, baseline=BASE_LEGIT, grounding_ok=True, status="done",
      prompt_version="v4", investigation_id=7, config=CFG, **changes):
    """route() on a clear legitimate v4 case, with fields of the agent verdict changed."""
    v = None if agent is None else dict(agent, **changes)
    return route(investigation_id, v, signals, baseline, grounding_ok, status, prompt_version, config)


# ---------- 1. one test per trigger ----------

@pytest.mark.parametrize("score", [30, 50, 70])
def test_uncertain_score(score):
    assert "uncertain_score" in r(risk_score=score).reasons


@pytest.mark.parametrize("score", [29.9, 70.1])
def test_score_outside_band_is_not_uncertain(score):
    assert "uncertain_score" not in r(risk_score=score).reasons


@pytest.mark.parametrize("agent, base, fires", [
    ("legitimate", "suspicious", True),
    ("legitimate", "likely_fraud", True),
    ("suspicious", "legitimate", True),
    ("likely_fraud", "legitimate", True),
    ("suspicious", "likely_fraud", False),   # both "not legitimate": they agree
    ("legitimate", "legitimate", False),
])
def test_agent_baseline_disagree(agent, base, fires):
    got = r(agent=dict(LEGIT, verdict=agent), baseline=dict(BASE_LEGIT, verdict=base))
    assert ("agent_baseline_disagree" in got.reasons) is fires


def test_amount_over_limit():
    assert "amount_over_limit" in r(signals=dict(SIGNALS, primary_amount=2500.01)).reasons
    assert "amount_over_limit" not in r(signals=dict(SIGNALS, primary_amount=2500)).reasons


def test_irreversible_action():
    assert "irreversible_action" in r(recommended_action="block_and_notify_customer").reasons


def test_low_confidence():
    assert "low_confidence" in r(confidence="low").reasons
    assert "low_confidence" not in r(confidence="medium").reasons


def test_ungrounded_evidence():
    assert "ungrounded_evidence" in r(grounding_ok=False).reasons


@pytest.mark.parametrize("status", ["failed", "dead"])
def test_agent_failed(status):
    got = r(agent=None, grounding_ok=None, status=status)
    assert "agent_failed" in got.reasons and got.review_status == "needs_review"


def test_new_account():
    assert "new_account" in r(signals=dict(SIGNALS, account_age_days=29)).reasons
    assert "new_account" not in r(signals=dict(SIGNALS, account_age_days=30)).reasons


def test_baseline_missing_blocks_auto_clear():
    got = r(baseline=None)
    assert got.reasons == ("baseline_missing",) and got.review_status == "needs_review"


def test_unknown_signals_fire_nothing():
    """No profile means no account age; no primary transaction means no amount. Neither
    is guessed. (A done row always has its primary transaction: the worker needs it.)"""
    assert r(signals=None).review_status == "auto_cleared"
    assert r(signals={"primary_amount": None, "account_age_days": None}).review_status == "auto_cleared"


# ---------- 2. v2 rows: no confidence or grounding triggers (H2 notes) ----------

@pytest.mark.parametrize("version", ["v2", "v3", None])
def test_confidence_and_grounding_skipped_before_v4(version):
    """v2 never asks for confidence (stored as low) and names no tools (grounding_ok NULL)."""
    got = r(prompt_version=version, confidence="low", grounding_ok=None)
    assert got.reasons == () and got.review_status == "auto_cleared"
    assert "ungrounded_evidence" not in r(prompt_version=version, grounding_ok=False).reasons


def test_v4_null_grounding_does_not_fire():
    assert "ungrounded_evidence" not in r(grounding_ok=None).reasons


# ---------- 3. outcomes, QA sampling, priority ----------

def test_outcome_auto_cleared():
    got = r(investigation_id=7)
    assert got == routing.Route("auto_cleared", (), 0, False)


def test_outcome_held_pending_review_for_hold():
    got = r(risk_score=50, verdict="suspicious", recommended_action="hold_for_review",
            baseline=dict(BASE_LEGIT, verdict="suspicious"))
    assert got.review_status == "held_pending_review" and got.reasons == ("uncertain_score",)


def test_outcome_held_pending_review_for_block():
    got = r(risk_score=90, verdict="likely_fraud", recommended_action="block_and_notify_customer",
            baseline=dict(BASE_LEGIT, verdict="likely_fraud"))
    assert got.review_status == "held_pending_review" and got.reasons == ("irreversible_action",)


def test_outcome_needs_review_for_approve_with_a_reason():
    got = r(signals=dict(SIGNALS, primary_amount=3000))
    assert got.review_status == "needs_review" and got.reasons == ("amount_over_limit",)


def test_not_legitimate_with_no_trigger_still_goes_to_review():
    """A narrower uncertain band in .env can leave a suspicious hold with no trigger."""
    narrow = RoutingConfig(uncertain_min=40, uncertain_max=60)
    got = r(risk_score=35, verdict="suspicious", recommended_action="hold_for_review",
            baseline=dict(BASE_LEGIT, verdict="suspicious"), config=narrow)
    assert got.reasons == ("verdict_not_legitimate",) and got.review_status == "held_pending_review"


@pytest.mark.parametrize("iid, sampled", [(10, True), (20, True), (0, True), (7, False), (11, False), (25, False)])
def test_qa_sample_is_every_tenth_auto_cleared_case(iid, sampled):
    assert r(investigation_id=iid).qa_sample is sampled


def test_qa_sample_rate_from_config():
    assert not any(r(investigation_id=i, config=RoutingConfig(qa_sample_rate=0)).qa_sample for i in range(100))
    assert sum(r(investigation_id=i, config=RoutingConfig(qa_sample_rate=0.25)).qa_sample
               for i in range(100)) == 25


def test_only_auto_cleared_cases_are_qa_sampled():
    assert not r(investigation_id=10, risk_score=50).qa_sample


def test_priority_formula():
    # 50 + 20 * 2 (uncertain_score, agent_baseline_disagree)
    assert r(risk_score=50, verdict="suspicious", recommended_action="hold_for_review").priority == 90
    # 90 + 20 * 2 (irreversible_action, agent_baseline_disagree) + 50
    assert r(risk_score=90, verdict="likely_fraud", recommended_action="block_and_notify_customer").priority == 180


def test_failed_rows_take_priority_from_the_baseline_score():
    got = r(agent=None, grounding_ok=None, status="dead", baseline=dict(BASE_LEGIT, risk_score=45))
    assert got.priority == 45 + 20


def test_qa_samples_sort_below_every_review_case():
    qa = r(investigation_id=10)
    assert qa.qa_sample and qa.priority == 0
    lowest_review = r(risk_score=0, signals=dict(SIGNALS, account_age_days=1))  # one reason, score 0
    assert lowest_review.review_status == "needs_review" and lowest_review.priority > qa.priority


def test_reasons_are_known_codes_in_a_fixed_order():
    got = r(risk_score=90, verdict="likely_fraud", recommended_action="block_and_notify_customer",
            confidence="low", grounding_ok=False, signals={"primary_amount": 9000, "account_age_days": 1})
    assert set(got.reasons) <= set(routing.REASONS)
    assert list(got.reasons) == [c for c in routing.REASONS if c in got.reasons]


@pytest.mark.parametrize("status", ["pending", "running"])
def test_unfinished_rows_are_not_routed(status):
    with pytest.raises(ValueError):
        r(status=status)


# ---------- 4. table-driven: clear legitimate clears, block never does ----------

@pytest.mark.parametrize("version", ["v2", "v4"])
def test_clear_legitimate_case_auto_clears(version):
    for score, amount, age in itertools.product([0, 10, 29], [1, 300, 2500], [30, 365, 5000]):
        got = r(prompt_version=version, risk_score=score, signals={"primary_amount": amount, "account_age_days": age})
        assert got.review_status == "auto_cleared", (version, score, amount, age, got)


def test_block_never_auto_clears():
    for score, verdict, base, conf, grounded, version, signals in itertools.product(
            range(0, 101, 5), ["legitimate", "suspicious", "likely_fraud"],
            [BASE_LEGIT, dict(BASE_LEGIT, verdict="likely_fraud"), None],
            ["low", "medium", "high"], [True, False, None], ["v2", "v4"],
            [SIGNALS, None]):
        got = r(risk_score=score, verdict=verdict, recommended_action="block_and_notify_customer",
                baseline=base, confidence=conf, grounding_ok=grounded, prompt_version=version, signals=signals)
        assert got.review_status == "held_pending_review" and "irreversible_action" in got.reasons, got
        assert not got.qa_sample


# ---------- 5. config ----------

def test_routing_config_from_env(monkeypatch):
    monkeypatch.setenv("REVIEW_AMOUNT_LIMIT_USD", "1000")
    monkeypatch.setenv("REVIEW_UNCERTAIN_MIN", "25")
    monkeypatch.setenv("REVIEW_UNCERTAIN_MAX", "75")
    monkeypatch.setenv("REVIEW_QA_SAMPLE_RATE", "0.2")
    monkeypatch.setenv("REVIEW_NEW_ACCOUNT_DAYS", "60")
    assert RoutingConfig.from_env() == RoutingConfig(1000, 25, 75, 0.2, 60)


def test_routing_config_defaults(monkeypatch):
    for name in ("AMOUNT_LIMIT_USD", "UNCERTAIN_MIN", "UNCERTAIN_MAX", "QA_SAMPLE_RATE", "NEW_ACCOUNT_DAYS"):
        monkeypatch.delenv(f"REVIEW_{name}", raising=False)
    assert RoutingConfig.from_env() == RoutingConfig(2500, 30, 70, 0.10, 30)


# ---------- 6-8. database: migration, worker, backfill ----------

@pytest.fixture(scope="module")
def h3_db():
    recreate_database(H3_DB)
    migrate(H3_DB)
    yield H3_DB
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{H3_DB}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(h3_db, monkeypatch):
    monkeypatch.setenv("AGENT_PGDATABASE", h3_db)
    tools.reset_pool()  # the baseline's read-only tools may be pooled on another test database
    conn = connect("admin", h3_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions, anomaly_flags, "
                    "account_profiles, merchant_risk RESTART IDENTITY")
    yield conn
    conn.close()


def add_incident(cur, user="u1", amount=45, status="pending", **cols):
    """One transaction, one flag, one incident at T0. Extra columns set on the incident row."""
    txn = f"{user}_t"
    cur.execute("""INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency,
                                             merchant, location_city, location_country, is_fraud)
                   VALUES (%s, %s, %s, %s, 'USD', 'Corner Shop', 'Toronto', 'CA', false)""",
                (txn, T0, user, amount))
    cur.execute("""INSERT INTO anomaly_flags (user_id, window_start, window_end, txn_count, total_amount)
                   VALUES (%s, %s, %s, 1, %s) RETURNING id""", (user, T0, T0 + timedelta(minutes=2), amount))
    flag = cur.fetchone()[0]
    cols = dict(status=status, **cols)
    names = ", ".join(cols)
    marks = ", ".join(["%s"] * len(cols))
    cur.execute(f"""INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids,
                                                primary_txn_id, {names})
                    VALUES (%s, %s, %s, %s, %s, %s, {marks}) RETURNING id""",
                (user, T0, T0 + timedelta(minutes=2), [flag], [txn], txn,
                 *[json.dumps(v) if isinstance(v, (dict, list)) else v for v in cols.values()]))
    return cur.fetchone()[0]


def routed(db, iid=None):
    with db.cursor() as cur:
        cur.execute("SELECT id, review_status, review_reasons, review_priority, qa_sample FROM investigations "
                    "WHERE id = coalesce(%s, id) ORDER BY id", (iid,))
        rows = cur.fetchall()
    return rows[0][1:] if iid is not None else rows


def test_review_status_check_constraint(db):
    with db.cursor() as cur:
        iid = add_incident(cur)
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute("UPDATE investigations SET review_status = 'blocked' WHERE id = %s", (iid,))
        cur.execute("SELECT review_status, review_reasons, review_priority, qa_sample FROM investigations")
        assert cur.fetchone() == (None, None, None, False)


def test_agent_ro_still_cannot_read_investigations(h3_db):
    conn = connect("agent_ro", h3_db)
    try:
        with conn.cursor() as cur, pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("SELECT review_status FROM investigations")
    finally:
        conn.close()


def make_worker(verdict=None, error=None, baseline=BASE_LEGIT, **cfg):
    def investigate(account_id, txn_id, merchant, amount, context=None, verbose=False):
        if error:
            raise error
        return {"verdict": dict(verdict), "trace": []}

    def run_baseline(account_id, txn_id, merchant, amount, verbose=False):
        return {"verdict": dict(baseline), "trace": []}

    config = dict(concurrency=1, hourly_budget=10_000, timeout_seconds=5, max_attempts=3,
                  backoff_seconds=0, stuck_after_seconds=600, poll_seconds=0.05)
    config.update(cfg)
    return Worker(WorkerConfig(**config), dbname=H3_DB, investigate=investigate, provider="fake",
                  model="fake-1", baseline=run_baseline if baseline else None, routing=CFG)


def test_worker_routes_a_done_row(db, monkeypatch):
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v2")
    with db.cursor() as cur:
        iid = add_incident(cur, amount=3000)
    make_worker(dict(LEGIT, explanation="x", evidence_cited=["x"])).run_until_idle()
    assert routed(db, iid) == ("needs_review", ["amount_over_limit"], 10 + 20, False)


def test_worker_auto_clears_a_clear_v2_case(db, monkeypatch):
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v2")
    with db.cursor() as cur:
        iid = add_incident(cur)
    make_worker(dict(LEGIT, explanation="x", evidence_cited=["x"])).run_until_idle()
    assert routed(db, iid) == ("auto_cleared", [], 0, False)


def test_worker_routes_a_failed_row(db):
    with db.cursor() as cur:
        iid = add_incident(cur)
    make_worker(error=ValueError("bad tool arguments")).run_until_idle()
    assert routed(db, iid) == ("needs_review", ["agent_failed"], 5 + 20, False)


def test_worker_routes_a_row_that_dies_after_retries(db):
    with db.cursor() as cur:
        iid = add_incident(cur)
    make_worker(error=TimeoutError("ollama"), max_attempts=1).run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT status FROM investigations WHERE id = %s", (iid,))
        assert cur.fetchone()[0] == "dead"
    assert routed(db, iid)[:2] == ("needs_review", ["agent_failed"])


def test_worker_routes_a_row_the_reaper_marks_dead(db):
    with db.cursor() as cur:
        iid = add_incident(cur, status="running", attempts=3, claimed_at=T0)
    make_worker(dict(LEGIT)).claim(1)
    with db.cursor() as cur:
        cur.execute("SELECT status FROM investigations WHERE id = %s", (iid,))
        assert cur.fetchone()[0] == "dead"
    assert routed(db, iid)[:2] == ("needs_review", ["agent_failed"])


def test_pending_and_running_rows_stay_unrouted(db):
    with db.cursor() as cur:
        add_incident(cur, user="u_pending")
        add_incident(cur, user="u_running", status="running", attempts=1, claimed_at=datetime.now(timezone.utc))
    route_existing.backfill(H3_DB, CFG)
    assert [row[1] for row in routed(db)] == [None, None]


def old_done_row(cur, user, amount=45, score=10, verdict="legitimate", action="approve"):
    """A done v2 row from before H1: no signals, no baseline, no route."""
    return add_incident(cur, user=user, amount=amount, status="done", prompt_version="v2",
                        risk_score=score, verdict=verdict, recommended_action=action, finished_at=T0)


def test_backfill_routes_old_rows_and_is_safe_to_rerun(db):
    with db.cursor() as cur:
        cur.execute("INSERT INTO account_profiles (user_id, home_city, avg_monthly_spend, account_opened_at, "
                    "source) VALUES ('u_old', 'Toronto', 500, %s, 'generator')", (T0 - timedelta(days=400),))
        clear = old_done_row(cur, "u_old")
        big = old_done_row(cur, "u_big", amount=4000, score=85, verdict="likely_fraud",
                           action="block_and_notify_customer")
        failed = add_incident(cur, user="u_failed", status="failed", error="x", finished_at=T0)
        dead = add_incident(cur, user="u_dead", status="dead", error="x", finished_at=T0)

    counts = route_existing.backfill(H3_DB, CFG)
    assert counts == {"auto_cleared": 1, "held_pending_review": 1, "needs_review": 2}

    with db.cursor() as cur:
        cur.execute("SELECT id, rule_hits, signals->>'account_age_days', baseline_verdict FROM investigations "
                    "ORDER BY id")
        filled = cur.fetchall()
    assert all(row[1] == ["spend_gt_300_in_2min"] or row[1] == [] for row in filled)
    assert filled[0][2] == "400" and all(row[3] is not None for row in filled)

    assert routed(db, clear)[0] == "auto_cleared"
    assert routed(db, big)[0] == "held_pending_review" and "irreversible_action" in routed(db, big)[1]
    assert routed(db, failed)[1][:1] == ["agent_failed"] and routed(db, dead)[1][:1] == ["agent_failed"]

    before = routed(db)
    assert route_existing.backfill(H3_DB, CFG) == {}
    assert routed(db) == before


def test_backfill_leaves_a_routed_row_alone(db):
    """A row already routed (or later reviewed) is never re-routed, even if config changes."""
    with db.cursor() as cur:
        iid = old_done_row(cur, "u_x")
        cur.execute("UPDATE investigations SET review_status = 'reviewed', review_reasons = '{}', "
                    "review_priority = 0 WHERE id = %s", (iid,))
    assert route_existing.backfill(H3_DB, RoutingConfig(uncertain_min=0, uncertain_max=100)) == {}
    assert routed(db, iid) == ("reviewed", [], 0, False)
