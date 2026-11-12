"""
review_data.py

Queries and the decision check behind the review pages (docs/PLAN_HITL.md Phase H5).
Everything here runs as the analyst role, through db.connect_kwargs, never as fraud_user.
The pages under pages/ only lay out what these functions return.

Writes go through record_decision() (migration 0013), the only path that closes a case.
validate_decision() runs first, so a form the database would reject (an override with no
reason, a missing final action) shows an inline error and nothing is sent.
"""
import statistics
import sys
from datetime import timedelta

import pandas as pd
import psycopg2
import psycopg2.extras

from db import ROOT, connect_kwargs

sys.path.insert(0, str(ROOT / "agent"))
from adapters.shared import evidence_items  # noqa: E402

DECISIONS = ("agree", "override", "request_info")
FINAL_ACTIONS = ("approve", "hold", "block", "request_otp")
MAX_REASON = 2000      # same caps as the CHECKs on analyst_decisions
MAX_ANALYST = 100

# Open for review: what record_decision() accepts as a first decision.
OPEN_CASE = ("(i.review_status IN ('needs_review', 'held_pending_review') "
             "OR (i.review_status = 'auto_cleared' AND i.qa_sample))")


class DecisionError(Exception):
    """A decision that was not recorded. The message is safe to show the analyst."""


def connect(dbname: str | None = None):
    conn = psycopg2.connect(**connect_kwargs("analyst", dbname))
    conn.autocommit = True
    return conn


def _frame(conn, sql, params=None) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def format_age(age: timedelta | None) -> str:
    if age is None or pd.isna(age):
        return ""
    minutes = int(age.total_seconds() // 60)
    days, minutes = divmod(minutes, 24 * 60)
    hours, minutes = divmod(minutes, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


# ---------- queue ----------

def queue_cases(conn) -> pd.DataFrame:
    """needs_review and held_pending_review cases plus open QA samples, highest
    review_priority first (QA samples sit at 0), oldest first within a priority."""
    df = _frame(conn, f"""
        SELECT i.id, i.user_id, coalesce(t.amount, (i.signals->>'primary_amount')::numeric) AS amount,
               i.verdict, i.risk_score, i.baseline_verdict, i.baseline_score, i.review_reasons,
               i.review_status, i.review_priority, i.qa_sample,
               now() - coalesce(i.finished_at, i.created_at) AS age,
               d.decision AS last_decision
        FROM investigations i
        LEFT JOIN v_agent_transactions t ON t.transaction_id = i.primary_txn_id
        LEFT JOIN v_case_latest_decision d ON d.investigation_id = i.id
        WHERE {OPEN_CASE}
        ORDER BY i.review_priority DESC NULLS LAST, coalesce(i.finished_at, i.created_at), i.id
    """)
    for col in ("amount", "risk_score", "baseline_score"):
        df[col] = pd.to_numeric(df[col]).astype(float)
    return df


# ---------- case detail ----------

def case_detail(conn, case_id: int) -> dict | None:
    """One case with its incident transactions, evidence, trace and decisions, or None."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, user_id, incident_start, incident_end, txn_ids, primary_txn_id, status, attempts,
                   error, finished_at, provider, model, prompt_version, risk_score, verdict,
                   recommended_action, confidence, grounding_ok, explanation, evidence, trace,
                   rule_hits, signals, baseline_score, baseline_verdict, baseline_action,
                   review_status, review_reasons, review_priority, qa_sample
            FROM investigations WHERE id = %s
        """, (case_id,))
        case = cur.fetchone()
    if case is None:
        return None
    case = dict(case)
    for col in ("risk_score", "baseline_score"):
        if case[col] is not None:
            case[col] = float(case[col])

    transactions = _frame(conn, """
        SELECT transaction_id, event_timestamp, amount, currency, merchant, merchant_category,
               card_type, location_city, location_country
        FROM v_agent_transactions
        WHERE transaction_id = ANY(%s)
        ORDER BY event_timestamp, transaction_id
    """, (list(case["txn_ids"] or []),))
    transactions["primary"] = transactions["transaction_id"] == case["primary_txn_id"]

    decisions = _frame(conn, """
        SELECT decided_at, analyst, decision, final_action, reason, agent_verdict_at_decision
        FROM analyst_decisions WHERE investigation_id = %s
        ORDER BY decided_at, id
    """, (case_id,))

    return {
        "case": case,
        "transactions": transactions,
        "evidence": evidence_items(case["evidence"]),
        "trace": case["trace"] if isinstance(case["trace"], list) else [],
        "decisions": decisions,
    }


# ---------- deciding ----------

def validate_decision(decision, final_action, reason, analyst) -> list[str]:
    """Problems to show next to the form; empty when record_decision() would accept it
    (apart from the case's own state, which only the database knows)."""
    errors = []
    reason = (reason or "").strip()
    if not (analyst or "").strip():
        errors.append("Enter your name.")
    elif len(analyst.strip()) > MAX_ANALYST:
        errors.append(f"Name is longer than {MAX_ANALYST} characters.")
    if decision not in DECISIONS:
        errors.append("Choose a decision: agree, override or request info.")
    if final_action is not None and final_action not in FINAL_ACTIONS:
        errors.append(f"Unknown final action {final_action!r}.")
    elif final_action is None and decision in ("agree", "override"):
        errors.append("Choose a final action.")
    if decision == "override" and not reason:
        errors.append("An override needs a reason.")
    if len(reason) > MAX_REASON:
        errors.append(f"Reason is longer than {MAX_REASON} characters.")
    return errors


def submit_decision(conn, case_id: int, analyst, decision, final_action, reason) -> int:
    """Check, then record through record_decision(). Returns the decision id.
    Raises DecisionError, and writes nothing, when the check or the database refuses."""
    errors = validate_decision(decision, final_action, reason, analyst)
    if errors:
        raise DecisionError(" ".join(errors))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT record_decision(%s, %s, %s, %s, %s)",
                        (case_id, analyst.strip(), decision, final_action, (reason or "").strip() or None))
            return cur.fetchone()[0]
    except (psycopg2.errors.NoDataFound, psycopg2.errors.ObjectNotInPrerequisiteState,
            psycopg2.errors.CheckViolation) as e:
        raise DecisionError(e.diag.message_primary) from e


# ---------- oversight ----------

def oversight(conn) -> dict:
    """Counts by review_status, queue depth, and, over cases closed by agree or override,
    the median time from routing to the closing decision and the override rate (latest
    closing decision per case). H6 adds v_oversight_metrics with the rest."""
    by_status = _frame(conn, """
        SELECT coalesce(review_status, 'not routed') AS review_status, count(*) AS n
        FROM investigations GROUP BY 1
    """)
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM investigations i WHERE {OPEN_CASE}")
        queue_depth = cur.fetchone()[0]
        cur.execute("""
            SELECT DISTINCT ON (investigation_id) decision
            FROM analyst_decisions
            WHERE decision IN ('agree', 'override')
            ORDER BY investigation_id, decided_at DESC, id DESC
        """)
        latest = [r[0] for r in cur.fetchall()]
        cur.execute("""
            SELECT DISTINCT ON (d.investigation_id) d.decided_at - coalesce(i.finished_at, i.created_at)
            FROM analyst_decisions d JOIN investigations i ON i.id = d.investigation_id
            WHERE d.decision IN ('agree', 'override')
            ORDER BY d.investigation_id, d.decided_at, d.id
        """)
        first_close = [r[0] for r in cur.fetchall()]
    return {
        "by_status": dict(zip(by_status["review_status"], by_status["n"].astype(int))),
        "queue_depth": queue_depth,
        "decided_cases": len(latest),
        "median_time_to_decision": statistics.median(first_close) if first_close else None,
        "override_rate": latest.count("override") / len(latest) if latest else None,
    }
