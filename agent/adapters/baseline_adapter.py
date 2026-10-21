"""
Rule-based baseline: no LLM, just a hand-tuned scoring function over the
SAME four tools the agents use.

Why this exists: "the agent got 13/15" means nothing without a point of
comparison. If a dozen if-statements score the same as the LLM, the
LLM is adding cost and latency but not value -- and you want to know
that before an interviewer asks. It also lets you test the eval
harness offline for free (MODEL_PROVIDER=baseline).

The rules are intentionally simple and a little naive. Where they fail
(e.g. they can't tell that 'Online' is not a city, or that a 2am
purchase is normal for a night-shift worker) is where an LLM should
be able to earn its keep.
"""
from datetime import datetime
from tools import TOOL_FUNCTIONS


def _score(account, pattern, merchant_risk, geo, txn_id, amount):
    points = 0
    reasons = []

    # Merchant risk
    if merchant_risk.get("risk_level") == "high":
        points += 50
        reasons.append("high-risk merchant")
    elif merchant_risk.get("risk_level") == "unknown":
        points += 10
        reasons.append("no risk data on merchant")

    # Impossible travel: different place, under 6 hours apart
    if geo.get("same_city") is False and geo.get("hours_between", 999) < 6:
        points += 50
        reasons.append("location change in under 6 hours")

    # Amount vs. monthly baseline
    avg = account.get("avg_monthly_spend") or 1
    ratio = amount / avg
    if ratio > 1:
        points += 25
        reasons.append(f"amount is {ratio:.1f}x average monthly spend")
    elif ratio > 0.5:
        points += 10
        reasons.append(f"amount is {ratio:.1f}x average monthly spend")

    # Young account
    if account.get("account_age_days", 9999) < 60:
        points += 10
        reasons.append("account under 60 days old")

    # Velocity: 3+ transactions in the 15 minutes before this one
    txns = pattern.get("transactions", [])
    this = next((t for t in txns if t["txn_id"] == txn_id), None)
    if this:
        t_now = datetime.fromisoformat(this["timestamp"])
        recent = [
            t for t in txns
            if t["txn_id"] != txn_id
            and 0 <= (t_now - datetime.fromisoformat(t["timestamp"])).total_seconds() <= 900
        ]
        if len(recent) >= 3:
            points += 40
            reasons.append(f"{len(recent)} transactions in the prior 15 minutes")

    return points, reasons


def run_investigation(account_id: str, txn_id: str, merchant: str, amount: float,
                      context: str | None = None, verbose: bool = True) -> dict:
    trace = []

    def call(name, **kwargs):
        result = TOOL_FUNCTIONS[name](**kwargs)
        trace.append({"step": 0, "type": "tool_call", "tool": name, "input": kwargs, "output": result})
        if verbose:
            print(f"  [tool] {name}({kwargs})")
        return result

    account = call("get_account_history", account_id=account_id)
    pattern = call("get_transaction_pattern", account_id=account_id)
    merchant_risk = call("check_merchant_risk", merchant=merchant)
    geo = call("get_geo_consistency", account_id=account_id, txn_id=txn_id)

    points, reasons = _score(account, pattern, merchant_risk, geo, txn_id, amount)

    if points >= 60:
        verdict, action = "likely_fraud", "block_and_notify_customer"
    elif points >= 25:
        verdict, action = "suspicious", "hold_for_review"
    else:
        verdict, action = "legitimate", "approve"

    result = {
        "risk_score": min(points, 100),
        "verdict": verdict,
        "explanation": "Rule-based score: " + ("; ".join(reasons) if reasons else "no risk signals triggered") + ".",
        "evidence_cited": reasons,
        "recommended_action": action,
    }
    trace.append({"step": 1, "type": "final_response", "content": "(rule-based, no LLM)"})
    return {"verdict": result, "trace": trace}
