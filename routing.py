"""
routing.py

Deterministic routing of a finished investigation (docs/PLAN_HITL.md Phase H3). Pure
functions only: no database, no LLM. The worker calls route() when it writes a result
and stores the outcome in the same transaction; route_existing.py backfills older rows.

Every case lands in one of three states:
  auto_cleared         no trigger fired, and the agent and the baseline both say legitimate.
                       A deterministic share (REVIEW_QA_SAMPLE_RATE) is marked qa_sample and
                       queued for review at the lowest priority.
  held_pending_review  a trigger fired and the recommended action is hold or block. The hold
                       is a recorded state for an analyst; nothing acts on the account.
  needs_review         any other case with a trigger.

A block_and_notify_customer recommendation always fires irreversible_action, so it can
never be auto-cleared.
"""
import os
from dataclasses import dataclass

# Reason codes, in the order they are listed on a case.
REASONS = (
    "uncertain_score",          # risk_score inside the uncertain band (inclusive)
    "agent_baseline_disagree",  # one of agent / baseline says legitimate and the other doesn't
    "amount_over_limit",        # primary_amount > REVIEW_AMOUNT_LIMIT_USD
    "irreversible_action",      # recommended block_and_notify_customer: always reviewed
    "low_confidence",           # v4 only: the model said low (or gave no confidence)
    "ungrounded_evidence",      # v4 only: grounding_ok is false
    "agent_failed",             # investigation failed or dead
    "new_account",              # account_age_days < REVIEW_NEW_ACCOUNT_DAYS; a proxy for "pattern looks new"
    "baseline_missing",         # done row with no baseline result, so it can't be cross-checked
    "verdict_not_legitimate",   # fallback: nothing else fired, but the verdict isn't legitimate
)

# Prompt versions whose output contract has confidence and tool-tied evidence. v2 (the
# default) asks for neither: its confidence is always stored as low and grounding_ok is
# NULL, so those two triggers would fire on every case or on none (H2 notes).
V4_CONTRACT = frozenset({"v4"})
HOLD_ACTIONS = frozenset({"hold_for_review", "block_and_notify_customer"})
ROUTED_STATUSES = frozenset({"done", "failed", "dead"})
PRIORITY_PER_REASON = 20
PRIORITY_IRREVERSIBLE = 50


@dataclass(frozen=True)
class RoutingConfig:
    amount_limit_usd: float = 2500.0   # about AED 9,200 (PLAN_HITL Q1)
    uncertain_min: float = 30.0        # uncertain band, inclusive (Q2)
    uncertain_max: float = 70.0
    qa_sample_rate: float = 0.10       # share of auto-cleared cases sent to QA (Q3)
    new_account_days: int = 30

    @classmethod
    def from_env(cls) -> "RoutingConfig":
        values = {}
        for name, field in cls.__dataclass_fields__.items():
            raw = os.getenv(f"REVIEW_{name.upper()}")
            if raw not in (None, ""):
                values[name] = type(field.default)(raw)
        return cls(**values)


@dataclass(frozen=True)
class Route:
    review_status: str
    reasons: tuple
    priority: int
    qa_sample: bool


def is_qa_sample(investigation_id: int, rate: float) -> bool:
    """Deterministic from the id: at 10%, every id divisible by 10."""
    if rate <= 0:
        return False
    return investigation_id % max(1, round(1 / rate)) == 0


def route(investigation_id: int, verdict: dict | None, signals: dict | None, baseline: dict | None,
          grounding_ok: bool | None, status: str, prompt_version: str | None,
          config: RoutingConfig) -> Route:
    """Route one finished investigation.

    verdict   the stored agent verdict (risk_score, verdict, recommended_action, confidence),
              or None when the investigation produced none
    signals   investigations.signals (H1); unknown values are None and fire nothing
    baseline  the rule baseline's result (risk_score, verdict), or None
    """
    if status not in ROUTED_STATUSES:
        raise ValueError(f"only finished investigations are routed, got status {status!r}")
    v = verdict or {}
    b = baseline or {}
    s = signals or {}
    score = v.get("risk_score")
    agent_verdict = v.get("verdict")
    action = v.get("recommended_action")

    fired = set()
    if score is not None and config.uncertain_min <= score <= config.uncertain_max:
        fired.add("uncertain_score")
    if agent_verdict and b.get("verdict") and (agent_verdict == "legitimate") != (b["verdict"] == "legitimate"):
        fired.add("agent_baseline_disagree")
    if s.get("primary_amount") is not None and float(s["primary_amount"]) > config.amount_limit_usd:
        fired.add("amount_over_limit")
    if action == "block_and_notify_customer":
        fired.add("irreversible_action")
    if prompt_version in V4_CONTRACT:
        if agent_verdict and (v.get("confidence") or "low") == "low":
            fired.add("low_confidence")
        if grounding_ok is False:
            fired.add("ungrounded_evidence")
    if status in ("failed", "dead"):
        fired.add("agent_failed")
    if s.get("account_age_days") is not None and s["account_age_days"] < config.new_account_days:
        fired.add("new_account")
    if status == "done" and not b.get("verdict"):
        fired.add("baseline_missing")
    if not fired and agent_verdict != "legitimate":
        fired.add("verdict_not_legitimate")
    reasons = tuple(code for code in REASONS if code in fired)

    if not reasons:
        return Route("auto_cleared", (), 0, is_qa_sample(investigation_id, config.qa_sample_rate))

    base_score = score if score is not None else b.get("risk_score") or 0
    priority = (round(float(base_score)) + PRIORITY_PER_REASON * len(reasons)
                + (PRIORITY_IRREVERSIBLE if "irreversible_action" in fired else 0))
    status_out = "held_pending_review" if action in HOLD_ACTIONS else "needs_review"
    return Route(status_out, reasons, priority, False)
