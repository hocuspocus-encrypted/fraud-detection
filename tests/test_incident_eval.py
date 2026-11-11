"""
Checks for the labeled incident eval (agent/incident_eval.py) and prompt v3
(evidence prefetch + score bands). No LLM calls: fake investigate functions
stand in for the model.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import json
import sys
from datetime import datetime, timedelta

import pytest

from db import ROOT, connect
from generate_transactions import TransactionGenerator

sys.path.insert(0, str(ROOT / "agent"))
import incident_eval as ie  # noqa: E402
from adapters import shared  # noqa: E402
from investigation_worker import Worker, WorkerConfig, valid_verdict  # noqa: E402

GOOD = {"risk_score": 85, "verdict": "likely_fraud", "explanation": "x",
        "evidence_cited": ["x"], "recommended_action": "block_and_notify_customer"}


# ---------- 1. the labeled eval set ----------

def test_stream_seed_changes_transactions_not_customers():
    a = TransactionGenerator(seed=42, n_users=200, stream_seed=1)
    b = TransactionGenerator(seed=42, n_users=200, stream_seed=2)
    assert a.profiles == b.profiles and a.catalog == b.catalog
    assert [r["transaction_id"] for r, _ in a.step()] != [r["transaction_id"] for r, _ in b.step()]


@pytest.fixture(scope="module")
def dev():
    return ie.build_split("dev")


def test_cases_are_deterministic_and_splits_disjoint(dev):
    cases, events = dev
    again, _ = ie.build_split("dev")
    assert cases == again
    test_cases, test_events = ie.build_split("test")
    assert {r["transaction_id"] for r, _ in events}.isdisjoint({r["transaction_id"] for r, _ in test_events})
    assert 60 <= len(cases) <= 120
    assert 110 <= len(test_cases) <= 220
    print(f"\ndev {len(cases)} cases, test {len(test_cases)} cases")


def test_case_fields(dev):
    cases, events = dev
    by_id = {r["transaction_id"]: r for r, _ in events}
    for c in cases:
        start, end = datetime.fromisoformat(c["incident_start"]), datetime.fromisoformat(c["incident_end"])
        assert datetime.fromisoformat(c["investigate_at"]) == end + timedelta(minutes=5)
        assert start >= ie.WARMUP_END                     # incidents only after the warm-up
        txns = [by_id[t] for t in c["txn_ids"]]
        assert all(start <= datetime.fromisoformat(r["timestamp"]) < end for r in txns)
        assert all(r["user_id"] == c["user_id"] for r in txns)
        primary = max(txns, key=lambda r: (r["amount"], r["timestamp"], r["transaction_id"]))
        assert c["primary_txn_id"] == primary["transaction_id"]
        assert c["fraud"] == any(r["is_fraud"] for r in txns)
    assert any(c["fraud"] for c in cases) and any(not c["fraud"] for c in cases)


@pytest.mark.parametrize("kinds, expected, acceptable", [
    ({"card_testing"}, "likely_fraud", ["suspicious"]),
    ({"high_risk_burst", "legit_new_merchant"}, "likely_fraud", ["suspicious"]),
    ({"legit_big_purchase"}, "legitimate", ["suspicious"]),
    ({"legit_travel"}, "legitimate", ["suspicious"]),
    ({"legit_new_merchant"}, "legitimate", ["suspicious"]),
    (set(), "legitimate", []),
])
def test_expected_verdicts(kinds, expected, acceptable):
    assert ie.expected_verdicts(kinds) == (expected, acceptable)


def test_runner_shows_history_only_up_to_investigation_time(dev):
    """Like live: when a case is investigated, the database holds transactions up to
    incident end + 5 min, not the rest of the stream."""
    cases, _ = dev
    seen = []

    def fake(account_id, txn_id, merchant, amount, context=None, verbose=False):
        c = connect("admin", ie.EVAL_DB)
        with c.cursor() as cur:
            cur.execute("SELECT max(event_timestamp) FROM transactions")
            seen.append((txn_id, cur.fetchone()[0]))
        c.close()
        return {"verdict": dict(GOOD), "trace": []}

    results = ie.run_split("dev", investigate=fake, provider="fake", model="fake", limit=8)
    assert [r["case_id"] for r in results] == [c["case_id"] for c in cases[:8]]
    assert all(r["status"] == "done" and r["verdict"] == "likely_fraud" for r in results)
    for c, (txn_id, newest) in zip(cases[:8], seen):
        assert txn_id == c["primary_txn_id"]
        assert newest <= datetime.fromisoformat(c["investigate_at"])
        assert newest >= max(datetime.fromisoformat(t) for t in c["txn_timestamps"])


def test_score_metrics():
    cases = [
        {"case_id": "f1", "fraud": True, "expected": "likely_fraud", "acceptable": ["suspicious"]},
        {"case_id": "f2", "fraud": True, "expected": "likely_fraud", "acceptable": ["suspicious"]},
        {"case_id": "l1", "fraud": False, "expected": "legitimate", "acceptable": ["suspicious"]},
        {"case_id": "o1", "fraud": False, "expected": "legitimate", "acceptable": []},
    ]

    def run(*verdicts_scores):
        return [{"case_id": c["case_id"], "status": "done", "verdict": v, "risk_score": s}
                for c, (v, s) in zip(cases, verdicts_scores)]

    runs = [run(("likely_fraud", 90), ("legitimate", 20), ("suspicious", 50), ("likely_fraud", 80)),
            run(("likely_fraud", 90), ("suspicious", 40), ("suspicious", 50), ("likely_fraud", 80))]
    m = ie.score(runs, cases)
    assert m["strict"] == pytest.approx((1 + 0 + 0 + 0 + 1 + 0 + 0 + 0) / 8)
    assert m["lenient"] == pytest.approx((1 + 0 + 1 + 0 + 1 + 1 + 1 + 0) / 8)
    assert m["fraud_caught"] == pytest.approx(3 / 4)       # f2 cleared in run 1
    assert m["legit_cleared"] == pytest.approx(0 / 4)
    assert m["good_blocked"] == pytest.approx(2 / 4)       # o1 blocked in both runs
    assert m["flip_rate"] == pytest.approx(1 / 4)          # f2 changed between runs
    # label derived from the score with the v3 bands: 90 lf, 20 leg, 50 sus, 80 lf | 90, 40 sus, 50, 80
    assert m["strict_from_score"] == pytest.approx((1 + 0 + 0 + 0 + 1 + 0 + 0 + 0) / 8)


# ---------- 2. evidence prefetch (prompt v3) ----------

def fake_tools(calls):
    def make(name):
        def fn(**kwargs):
            calls.append((name, kwargs))
            return {"tool": name, **kwargs}
        return fn
    return {n: make(n) for n in ("get_account_history", "get_transaction_pattern",
                                 "check_merchant_risk", "get_geo_consistency")}


def test_gather_evidence_runs_each_tool_once():
    calls = []
    trace = shared.gather_evidence("acc_1", "t_9", "Shop", fake_tools(calls))
    assert [name for name, _ in calls] == ["get_account_history", "get_transaction_pattern",
                                           "check_merchant_risk", "get_geo_consistency"]
    assert calls[3][1] == {"account_id": "acc_1", "txn_id": "t_9"}
    assert all(s["type"] == "tool_call" and s["prefetched"] for s in trace)
    assert [s["tool"] for s in trace] == [n for n, _ in calls]


def test_v2_task_unchanged_and_v3_task_carries_the_evidence():
    v2 = shared.build_task("acc_1", "t_9", "Shop", 12.5, "ctx")
    assert v2 == ("Investigate this flagged transaction:\n- account_id: acc_1\n- txn_id: t_9\n"
                  "- merchant: Shop\n- amount: 12.5\n\nContext from the detection pipeline:\nctx\n")
    evidence = shared.gather_evidence("acc_1", "t_9", "Shop", fake_tools([]))
    v3 = shared.build_task("acc_1", "t_9", "Shop", 12.5, "ctx", evidence=evidence)
    assert v3.startswith(v2)
    for step in evidence:
        assert step["tool"] in v3 and json.dumps(step["output"]) in v3


def test_v3_system_prompt_states_the_bands():
    prompt = shared.system_prompt("v3")
    for verdict, (lo, hi) in shared.VERDICT_BANDS.items():
        assert f"{lo}-{hi}" in prompt and verdict in prompt
    assert shared.system_prompt("v2") == shared.SYSTEM_PROMPT


# ---------- 3. verdict follows score ----------

@pytest.mark.parametrize("score, verdict, ok_v3", [
    (85, "likely_fraud", True), (85, "suspicious", False), (50, "suspicious", True),
    (30, "legitimate", True), (31, "legitimate", False), (71, "likely_fraud", True), (70, "likely_fraud", False),
])
def test_band_consistency(score, verdict, ok_v3):
    action = {"legitimate": "approve", "suspicious": "hold_for_review",
              "likely_fraud": "block_and_notify_customer"}[verdict]
    v = {"risk_score": score, "verdict": verdict, "recommended_action": action}
    assert valid_verdict(v, prompt_version="v3") is ok_v3
    assert valid_verdict(v, prompt_version="v2") is True


@pytest.mark.parametrize("script, status", [
    ([dict(GOOD, verdict="suspicious", recommended_action="hold_for_review"), dict(GOOD)], "done"),
    ([dict(GOOD, verdict="suspicious", recommended_action="hold_for_review")], "failed"),
])
def test_worker_retries_a_verdict_that_contradicts_its_score(monkeypatch, script, status):
    """Under v3, score 85 labeled 'suspicious' is treated like unparseable output: one retry, then failed."""
    monkeypatch.setenv("AGENT_PROMPT_VERSION", "v3")
    calls = []

    def fake(account_id, txn_id, merchant, amount, context=None, verbose=False):
        calls.append(txn_id)
        return {"verdict": dict(script[min(len(calls), len(script)) - 1]), "trace": []}

    results = ie.run_split("dev", investigate=fake, provider="fake", model="fake", limit=1)
    assert results[0]["status"] == status
    assert results[0]["prompt_version"] == "v3"
    assert len(calls) == 2
