"""
docs/PLAN_HITL.md Phase H2: verdict contract v4 (confidence, evidence tied to a tool,
grounding check) and the incident eval filling rule_hits. No LLM calls; the database
checks use throwaway databases.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import json
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from db import ROOT, connect
from db.migrate import migrate, recreate_database

sys.path.insert(0, str(ROOT / "agent"))
import incident_eval as ie  # noqa: E402
from adapters import ollama_adapter, shared  # noqa: E402
from investigation_worker import Worker, WorkerConfig, normalize_verdict, valid_verdict  # noqa: E402
from simulate_flags import SLIDE_SECONDS, WINDOW_SECONDS, find_incidents, flagged_windows  # noqa: E402

H2_DB = "fraud_h2_test"
T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)

V4 = {"risk_score": 80, "verdict": "likely_fraud", "confidence": "high", "explanation": "Fake.",
      "evidence_cited": [{"claim": "merchant is high risk", "tool": "check_merchant_risk"},
                         {"claim": "far from home", "tool": "get_geo_consistency"}],
      "recommended_action": "block_and_notify_customer"}


def call(tool, output=None):
    return {"step": 0, "type": "tool_call", "tool": tool, "input": {}, "output": output or {"ok": True}}


# ---------- 1. the v4 prompt ----------

def test_v4_prompt_asks_for_confidence_and_tool_per_claim():
    prompt = shared.system_prompt("v4")
    assert prompt == shared.SYSTEM_PROMPT_V4
    assert '"confidence": "<one of: low, medium, high>"' in prompt
    assert '{"claim": ' in prompt and '"tool": ' in prompt


def test_v2_prompt_is_unchanged():
    assert shared.system_prompt("v2") == shared.SYSTEM_PROMPT
    assert "confidence" not in shared.SYSTEM_PROMPT


def test_v4_is_a_known_version_and_v2_still_the_default(monkeypatch):
    assert "v4" in shared.PROMPT_VERSIONS
    monkeypatch.delenv("AGENT_PROMPT_VERSION", raising=False)
    assert shared.prompt_version() == "v2"


def test_ollama_adapter_sends_v4_prompt_and_returns_the_new_fields(monkeypatch):
    """Adapter round trip under v4 against a fake Ollama: the new fields come back as given."""
    import httpx
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v4")
    monkeypatch.setenv("OLLAMA_RECYCLE_AFTER_REQUESTS", "0")
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"message": {"role": "assistant", "content": json.dumps(V4)}, "done": True})

    monkeypatch.setattr(ollama_adapter, "_make_client",
                        lambda: httpx.Client(base_url="http://ollama.test", transport=httpx.MockTransport(handler)))
    result = ollama_adapter.run_investigation("acc_1", "t_1", "Shop", 10.0, verbose=False)
    assert requests[0]["messages"][0]["content"] == shared.SYSTEM_PROMPT_V4
    assert result["verdict"]["confidence"] == "high"
    assert result["verdict"]["evidence_cited"][0] == {"claim": "merchant is high risk", "tool": "check_merchant_risk"}


# ---------- 2. reading verdicts: confidence and evidence ----------

@pytest.mark.parametrize("given, stored", [
    ("high", "high"), ("Medium", "medium"), ("low", "low"),
    (None, "low"),          # missing is low, not a failure
    ("very sure", "low"),   # not one of the three: low too
])
def test_confidence_missing_or_unknown_is_low(given, stored):
    v = dict(V4)
    if given is None:
        del v["confidence"]
    else:
        v["confidence"] = given
    assert valid_verdict(v, prompt_version="v4")
    assert normalize_verdict(v)["confidence"] == stored


def test_v4_verdict_still_needs_score_label_and_action():
    assert not valid_verdict(dict(V4, risk_score=150), prompt_version="v4")
    assert not valid_verdict(dict(V4, verdict="maybe"), prompt_version="v4")
    assert not valid_verdict(dict(V4, recommended_action="freeze"), prompt_version="v4")


def test_evidence_items_reads_old_strings_and_new_objects():
    assert shared.evidence_items(["high-risk merchant", "far from home"]) == [
        {"claim": "high-risk merchant", "tool": None}, {"claim": "far from home", "tool": None}]
    assert shared.evidence_items(V4["evidence_cited"]) == V4["evidence_cited"]
    assert shared.evidence_items([{"claim": "x"}, "y", {"tool": "check_merchant_risk"}]) == [
        {"claim": "x", "tool": None}, {"claim": "y", "tool": None}, {"claim": "", "tool": "check_merchant_risk"}]
    assert shared.evidence_items(None) == [] and shared.evidence_items("one string") == [
        {"claim": "one string", "tool": None}]


# ---------- 3. grounding ----------

def test_grounded_when_every_cited_tool_was_called():
    trace = [call("check_merchant_risk"), call("get_geo_consistency"), call("get_account_history")]
    assert shared.grounding_ok(V4, trace, "v4") is True


def test_citing_a_tool_that_was_never_called_is_ungrounded():
    trace = [call("check_merchant_risk")]
    assert shared.grounding_ok(V4, trace, "v4") is False


def test_citing_a_tool_whose_call_failed_is_ungrounded():
    trace = [call("check_merchant_risk"), call("get_geo_consistency", {"error": "Bad arguments"})]
    assert shared.grounding_ok(V4, trace, "v4") is False


@pytest.mark.parametrize("evidence", [
    [],                                                   # nothing cited
    ["merchant is high risk"],                            # a v4 claim with no tool
    [{"claim": "merchant is high risk"}],
    "merchant is high risk",                              # not a list
])
def test_v4_claim_without_a_tool_is_ungrounded(evidence):
    trace = [call("check_merchant_risk")]
    assert shared.grounding_ok(dict(V4, evidence_cited=evidence), trace, "v4") is False


def test_prefetched_calls_count_as_called():
    trace = [dict(call("check_merchant_risk"), prefetched=True), call("get_geo_consistency")]
    assert shared.grounding_ok(V4, trace, "v4") is True


def test_grounding_is_not_checked_for_v2_strings():
    v2 = dict(V4, evidence_cited=["merchant is high risk"])
    del v2["confidence"]
    assert shared.grounding_ok(v2, [], "v2") is None


# ---------- 4. the worker stores confidence and grounding_ok ----------

@pytest.fixture(scope="module")
def h2_db():
    recreate_database(H2_DB)
    migrate(H2_DB)
    yield H2_DB
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{H2_DB}" WITH (FORCE)')
    admin.close()


@pytest.fixture
def db(h2_db):
    conn = connect("admin", h2_db)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analyst_decisions, investigation_attempts, investigations, transactions, anomaly_flags RESTART IDENTITY")
        cur.execute("""INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency,
                                                 merchant, location_city, location_country, is_fraud)
                       VALUES ('t1', %s, 'u1', 400, 'USD', 'Gadget Hub', 'Paris', 'FR', false)""", (T0,))
        cur.execute("""INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids,
                                                   primary_txn_id)
                       VALUES ('u1', %s, %s, '{}', '{t1}', 't1')""", (T0, T0 + timedelta(minutes=2)))
    yield conn
    conn.close()


def run_worker(verdict, trace):
    def investigate(account_id, txn_id, merchant, amount, context=None, verbose=False):
        return {"verdict": json.loads(json.dumps(verdict)), "trace": list(trace)}
    cfg = WorkerConfig(concurrency=1, hourly_budget=10_000, timeout_seconds=5, max_attempts=3,
                       backoff_seconds=0, stuck_after_seconds=600, poll_seconds=0.05)
    Worker(cfg, dbname=H2_DB, investigate=investigate, provider="fake", model="fake-1").run_until_idle()


def stored(db):
    with db.cursor() as cur:
        cur.execute("SELECT status, prompt_version, confidence, grounding_ok, evidence FROM investigations")
        return cur.fetchone()


def test_worker_stores_confidence_and_grounding(db, monkeypatch):
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v4")
    run_worker(V4, [call("check_merchant_risk"), call("get_geo_consistency")])
    assert stored(db) == ("done", "v4", "high", True, V4["evidence_cited"])


def test_ungrounded_verdict_is_stored_not_failed(db, monkeypatch):
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v4")
    no_conf = {k: v for k, v in V4.items() if k != "confidence"}
    run_worker(no_conf, [call("check_merchant_risk")])
    assert stored(db)[:4] == ("done", "v4", "low", False)


def test_v2_row_keeps_string_evidence_and_skips_grounding(db, monkeypatch):
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v2")
    v2 = {k: v for k, v in V4.items() if k != "confidence"}
    v2["evidence_cited"] = ["merchant is high risk"]
    run_worker(v2, [call("check_merchant_risk")])
    assert stored(db) == ("done", "v2", "low", None, ["merchant is high risk"])


def test_agent_ro_still_cannot_read_investigations(h2_db):
    conn = connect("agent_ro", h2_db)
    try:
        with conn.cursor() as cur, pytest.raises(psycopg2.errors.InsufficientPrivilege):
            cur.execute("SELECT grounding_ok, confidence FROM investigations")
    finally:
        conn.close()


# ---------- 5. the incident eval inserts the simulated flag windows ----------

@pytest.fixture(scope="module")
def dev():
    return ie.build_split("dev")


def test_flagged_windows_rebuild_find_incidents(dev):
    """find_incidents is the merge of flagged_windows, so the eval's flags match its incidents."""
    _, events = dev
    windows = flagged_windows(events)
    for user, start, end in find_incidents(events):
        mine = [w for w in windows[user] if start <= w[0] and w[0] + WINDOW_SECONDS <= end]
        assert mine and min(w[0] for w in mine) == start and max(w[0] for w in mine) + WINDOW_SECONDS == end
        assert all(w[0] % SLIDE_SECONDS == 0 for w in mine)


def test_cases_carry_their_flag_windows(dev):
    cases, events = dev
    for c in cases:
        start, end = datetime.fromisoformat(c["incident_start"]), datetime.fromisoformat(c["incident_end"])
        assert c["flags"]
        for f in c["flags"]:
            ws, we = datetime.fromisoformat(f["window_start"]), datetime.fromisoformat(f["window_end"])
            assert start <= ws and we <= end and we - ws == timedelta(seconds=WINDOW_SECONDS)
            assert f["txn_count"] > 2 or f["total_amount"] > 300
        assert min(datetime.fromisoformat(f["window_start"]) for f in c["flags"]) == start
        assert max(datetime.fromisoformat(f["window_end"]) for f in c["flags"]) == end


def test_eval_fills_rule_hits_and_v4_context_names_them(dev, monkeypatch):
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v4")
    cases, _ = dev
    contexts = []

    def fake(account_id, txn_id, merchant, amount, context=None, verbose=False):
        contexts.append(context)
        return {"verdict": dict(V4), "trace": [call("check_merchant_risk"), call("get_geo_consistency")]}

    results = ie.run_split("dev", investigate=fake, provider="fake", model="fake", limit=6)
    conn = connect("admin", ie.EVAL_DB)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT flag_ids, rule_hits FROM investigations ORDER BY id")
            rows = cur.fetchall()
            cur.execute("SELECT count(*) FROM anomaly_flags")
            n_flags = cur.fetchone()[0]
    finally:
        conn.close()
    assert len(rows) == 6 and n_flags == sum(len(c["flags"]) for c in cases[:6])
    for (flag_ids, rule_hits), context, case in zip(rows, contexts, cases[:6]):
        assert len(flag_ids) == len(case["flags"])
        assert rule_hits and set(rule_hits) <= {"velocity_gt_2_in_2min", "spend_gt_300_in_2min"}
        assert all(hit in context for hit in rule_hits)
    assert all(r["grounding_ok"] is True and r["confidence"] == "high" for r in results)
