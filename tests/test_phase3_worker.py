"""
Phase 3 acceptance checks (PLAN.md, Phase 3): the investigation worker.

Fake investigate functions stand in for the LLM, so nothing here calls an API.
The kill/restart check runs the real worker process with the rule-based
baseline provider.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

from db import ROOT, connect
from db.migrate import migrate, recreate_database
from investigation_worker import Worker, WorkerConfig, is_enabled

WORKER_DB = "fraud_phase3_test"

GOOD = {
    "risk_score": 80,
    "verdict": "likely_fraud",
    "explanation": "Fake verdict for tests.",
    "evidence_cited": ["fake evidence"],
    "recommended_action": "block_and_notify_customer",
}


class FakeAPIError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class Recorder:
    """A fake investigate(): returns or raises from a script, and records calls."""

    def __init__(self, *script, delay=0.0):
        self.script = list(script) or [GOOD]
        self.calls = []
        self.delay = delay

    def __call__(self, account_id, txn_id, merchant, amount, context=None, verbose=False):
        self.calls.append(txn_id)
        if self.delay:
            time.sleep(self.delay)
        step = self.script[min(len(self.calls), len(self.script)) - 1]
        if isinstance(step, Exception):
            raise step
        return {"verdict": dict(step), "trace": [{"step": 0, "type": "tool_call", "tool": "fake",
                                                   "input": {"txn_id": txn_id}, "output": {}}],
                "usage": {"turns": 2, "total_tokens": 1234}}


@pytest.fixture(scope="module")
def worker_db():
    recreate_database(WORKER_DB)
    migrate(WORKER_DB)
    yield WORKER_DB
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{WORKER_DB}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(worker_db):
    conn = connect("admin", worker_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions RESTART IDENTITY")
    yield conn
    conn.close()


def make_incidents(db, n, prefix="w"):
    """n pending incidents, each with one transaction (what the enqueuer would produce)."""
    t = datetime(2026, 10, 18, 10, 0, tzinfo=timezone.utc)
    with db.cursor() as cur:
        for i in range(n):
            user, txn, ts = f"{prefix}_user_{i:04d}", f"{prefix}_txn_{i:04d}", t + timedelta(minutes=i)
            cur.execute(
                """INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency,
                                             merchant, location_city, is_fraud)
                   VALUES (%s, %s, %s, 450, 'USD', 'Some Merchant', 'Testville', false)""",
                (txn, ts, user),
            )
            cur.execute(
                """INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids, primary_txn_id)
                   VALUES (%s, %s, %s, '{1}', ARRAY[%s]::varchar[], %s)""",
                (user, ts - timedelta(minutes=1), ts + timedelta(minutes=2), txn, txn),
            )


def fast_config(**overrides):
    cfg = dict(concurrency=2, hourly_budget=10_000, timeout_seconds=5, max_attempts=3,
               backoff_seconds=0, stuck_after_seconds=600, poll_seconds=0.05)
    cfg.update(overrides)
    return WorkerConfig(**cfg)


def make_worker(fake, **overrides):
    return Worker(fast_config(**overrides), dbname=WORKER_DB, investigate=fake,
                  provider="fake", model="fake-model-1")


def statuses(db):
    with db.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM investigations GROUP BY status")
        return dict(cur.fetchall())


# ---------- happy path ----------

def test_fifty_incidents_finish_exactly_once(db):
    make_incidents(db, 50)
    fake = Recorder()
    make_worker(fake).run_until_idle()

    assert statuses(db) == {"done": 50}
    assert len(fake.calls) == 50 and len(set(fake.calls)) == 50
    with db.cursor() as cur:
        cur.execute("SELECT count(*), count(DISTINCT investigation_id) FROM investigation_attempts WHERE outcome = 'done'")
        assert cur.fetchone() == (50, 50)


def test_verdict_and_trace_are_stored(db):
    make_incidents(db, 1)
    make_worker(Recorder()).run_until_idle()
    with db.cursor() as cur:
        cur.execute("""SELECT status, provider, model, prompt_version, risk_score, verdict, recommended_action,
                              explanation, evidence, trace, attempts, finished_at, error, usage
                       FROM investigations""")
        (status, provider, model, prompt_version, risk_score, verdict, action,
         explanation, evidence, trace, attempts, finished_at, error, usage) = cur.fetchone()
    assert usage == {"turns": 2, "total_tokens": 1234}
    assert (status, provider, model, attempts, error) == ("done", "fake", "fake-model-1", 1, None)
    assert prompt_version
    assert (float(risk_score), verdict, action) == (80.0, "likely_fraud", "block_and_notify_customer")
    assert explanation == GOOD["explanation"]
    assert evidence == ["fake evidence"]
    assert trace[0]["tool"] == "fake"
    assert finished_at is not None


def test_incident_context_reaches_the_agent(db):
    make_incidents(db, 1)
    seen = {}

    def fake(account_id, txn_id, merchant, amount, context=None, verbose=False):
        seen.update(account_id=account_id, txn_id=txn_id, merchant=merchant, amount=amount, context=context)
        return {"verdict": dict(GOOD), "trace": []}

    make_worker(fake).run_until_idle()
    assert (seen["account_id"], seen["txn_id"], seen["merchant"], seen["amount"]) == \
        ("w_user_0000", "w_txn_0000", "Some Merchant", 450.0)
    assert "1 transaction" in seen["context"] and "450.00" in seen["context"]


@pytest.mark.parametrize("score,expected", [
    (15, ("legitimate", "approve")),
    (30, ("legitimate", "approve")),
    (50, ("suspicious", "hold_for_review")),
    (71, ("likely_fraud", "block_and_notify_customer")),
])
def test_verdict_and_action_follow_the_score(db, score, expected):
    """The model's own label is kept for audit, but the stored verdict and action come
    from its risk_score: on the dev split qwen3:4b-instruct labelled 92% of non-fraud
    'suspicious' while its scores separated the classes well."""
    make_incidents(db, 1)
    hedged = dict(GOOD, risk_score=score, verdict="suspicious", recommended_action="hold_for_review")
    make_worker(Recorder(hedged)).run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT status, risk_score, verdict, recommended_action, trace FROM investigations")
        status, risk_score, verdict, action, trace = cur.fetchone()
    assert (status, float(risk_score), (verdict, action)) == ("done", float(score), expected)
    assert trace[0]["tool"] == "fake"
    assert trace[-1] == {"type": "verdict_from_score", "risk_score": score,
                         "model_verdict": "suspicious", "model_action": "hold_for_review",
                         "verdict": expected[0], "recommended_action": expected[1]}


# ---------- failures ----------

def test_transient_errors_retry_then_dead(db):
    make_incidents(db, 1)
    fake = Recorder(FakeAPIError(503))
    make_worker(fake).run_until_idle()

    assert statuses(db) == {"dead": 1}
    assert len(fake.calls) == 3
    with db.cursor() as cur:
        cur.execute("SELECT attempts, error FROM investigations")
        attempts, error = cur.fetchone()
        cur.execute("SELECT outcome FROM investigation_attempts ORDER BY attempt")
        outcomes = [r[0] for r in cur.fetchall()]
    assert attempts == 3 and "503" in error
    assert outcomes == ["transient_error"] * 3


def test_rate_limit_then_success(db):
    make_incidents(db, 1)
    fake = Recorder(FakeAPIError(429), GOOD)
    make_worker(fake).run_until_idle()
    assert statuses(db) == {"done": 1}
    with db.cursor() as cur:
        cur.execute("SELECT attempts FROM investigations")
        assert cur.fetchone()[0] == 2


def test_backoff_delays_the_retry(db):
    make_incidents(db, 1)
    make_worker(Recorder(FakeAPIError(503)), backoff_seconds=60).run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT status, attempts, next_attempt_at - now() FROM investigations")
        status, attempts, wait = cur.fetchone()
    assert (status, attempts) == ("pending", 1)
    assert timedelta(seconds=50) < wait <= timedelta(seconds=60)


def test_unparseable_output_gets_one_retry_then_failed(db):
    make_incidents(db, 2, prefix="p")
    bad = {"raw_output": "not json", "parse_error": True}
    # first incident: bad, then good -> done. second: bad twice -> failed.
    fake = Recorder(bad, GOOD, bad, bad)
    make_worker(fake, concurrency=1).run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT user_id, status, error FROM investigations ORDER BY id")
        rows = cur.fetchall()
    assert rows[0][1] == "done"
    assert rows[1][1] == "failed" and "unparseable" in rows[1][2]


def test_invalid_verdict_counts_as_unparseable(db):
    make_incidents(db, 1)
    make_worker(Recorder(dict(GOOD, verdict="definitely_fraud"))).run_until_idle()
    assert statuses(db) == {"failed": 1}


def test_tool_failure_is_permanent(db):
    make_incidents(db, 1)
    fake = Recorder(RuntimeError("tool exploded"))
    make_worker(fake).run_until_idle()
    assert statuses(db) == {"failed": 1}
    assert len(fake.calls) == 1


def test_max_iterations_is_permanent(db):
    make_incidents(db, 1)
    make_worker(Recorder({"error": "Max iterations reached without a final verdict"})).run_until_idle()
    assert statuses(db) == {"failed": 1}


def test_timeout_counts_as_transient(db):
    make_incidents(db, 1)
    make_worker(Recorder(delay=1.0), timeout_seconds=0.2, max_attempts=1).run_until_idle()
    assert statuses(db) == {"dead": 1}
    with db.cursor() as cur:
        cur.execute("SELECT outcome FROM investigation_attempts")
        assert cur.fetchone()[0] == "timeout"


# ---------- controls ----------

def test_budget_caps_attempts_per_hour(db):
    make_incidents(db, 15)
    fake = Recorder()
    make_worker(fake, hourly_budget=10).run_until_idle()
    assert statuses(db) == {"done": 10, "pending": 5}
    assert len(fake.calls) == 10
    # a restarted worker shares the same budget (it is counted in the database)
    make_worker(fake, hourly_budget=10).run_until_idle()
    assert statuses(db) == {"done": 10, "pending": 5}


def test_kill_switch(db, monkeypatch):
    make_incidents(db, 3)
    monkeypatch.setenv("WORKER_ENABLED", "false")
    assert is_enabled() is False
    fake = Recorder()
    make_worker(fake).run_until_idle()
    assert statuses(db) == {"pending": 3} and fake.calls == []
    monkeypatch.setenv("WORKER_ENABLED", "true")
    assert is_enabled() is True


# ---------- crash recovery ----------

def test_stuck_rows_are_reclaimed_and_stale_results_discarded(db):
    make_incidents(db, 3)
    crashed = make_worker(Recorder(dict(GOOD, verdict="legitimate", recommended_action="approve")))
    claimed = crashed.claim(3)          # claimed, then the worker "dies" without finishing
    assert len(claimed) == 3
    with db.cursor() as cur:
        cur.execute("UPDATE investigations SET claimed_at = now() - interval '1 hour'")

    make_worker(Recorder(), stuck_after_seconds=60).run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT status, attempts, verdict FROM investigations")
        assert set(cur.fetchall()) == {("done", 2, "likely_fraud")}
        cur.execute("SELECT attempt, outcome FROM investigation_attempts ORDER BY investigation_id, attempt")
        assert cur.fetchall() == [(1, "abandoned"), (2, "done")] * 3

    crashed.process(claimed[0])         # the old worker comes back late: its result must not land
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM investigations WHERE verdict = 'likely_fraud'")
        assert cur.fetchone()[0] == 3


def test_stuck_row_at_max_attempts_goes_dead(db):
    make_incidents(db, 1)
    make_worker(Recorder(), max_attempts=1).claim(1)
    with db.cursor() as cur:
        cur.execute("UPDATE investigations SET claimed_at = now() - interval '1 hour'")
    make_worker(Recorder(), max_attempts=1, stuck_after_seconds=60).run_until_idle()
    with db.cursor() as cur:
        cur.execute("SELECT status, error FROM investigations")
        status, error = cur.fetchone()
    assert status == "dead" and "stuck" in error


def test_kill_worker_process_mid_run_and_restart(db):
    """Real process, real tools (baseline provider): kill -9 partway, restart, nothing lost or duplicated."""
    n = 400
    make_incidents(db, n, prefix="k")
    env = dict(os.environ, MODEL_PROVIDER="baseline", AGENT_PGDATABASE=WORKER_DB, WORKER_ENABLED="true",
               WORKER_HOURLY_BUDGET="100000", WORKER_BACKOFF_SECONDS="0", WORKER_POLL_SECONDS="0.05",
               WORKER_TIMEOUT_SECONDS="1", WORKER_STUCK_AFTER_SECONDS="2")
    cmd = [sys.executable, str(ROOT / "investigation_worker.py"), "--dbname", WORKER_DB, "--until-idle"]

    proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 60
    while time.time() < deadline:
        done = statuses(db).get("done", 0)
        if done >= 40:
            break
        time.sleep(0.05)
    proc.kill()
    proc.wait()
    at_kill = statuses(db)
    print(f"queue at kill: {at_kill}")
    assert 0 < at_kill.get("done", 0) < n, f"kill did not land mid-run: {at_kill}"

    rerun = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
    assert rerun.returncode == 0, rerun.stderr[-2000:]

    assert statuses(db) == {"done": n}
    with db.cursor() as cur:
        cur.execute("""SELECT count(*) FILTER (WHERE outcome = 'done'),
                              count(DISTINCT investigation_id) FILTER (WHERE outcome = 'done'),
                              count(*) FILTER (WHERE outcome = 'abandoned')
                       FROM investigation_attempts""")
        done_attempts, distinct_done, abandoned = cur.fetchone()
    assert done_attempts == distinct_done == n
    assert abandoned == at_kill.get("running", 0)
