"""
Each tool is a plain Python function plus a JSON schema describing it
to the model. Keep tools narrow and single-purpose -- an agent is only
as good as the clarity of the tools you hand it.

These bodies read the pipeline's Postgres (PLAN.md Phase 2). The original
SQLite versions are kept in tools_sqlite.py as the parity reference; the
schemas below are unchanged from them.

Guardrails:
  - connects only as agent_ro (SELECT on an allow-list, read-only, 5 s statement timeout)
  - reads transactions through v_agent_transactions, which has no is_fraud label
  - parameterized queries only; free-text inputs and outputs are length-capped
  - the pattern tool returns at most MAX_PATTERN_ROWS rows

Pipeline column names are aliased to the scaffold's names:
user_id -> account_id, transaction_id -> txn_id, event_timestamp -> timestamp,
location_city -> city. Timestamps are returned as UTC ISO-8601 ("...Z").

Settings: AGENT_PGDATABASE (default: PGDATABASE from .env).
"""
import os
import sys
import threading
from datetime import timezone
from pathlib import Path

from psycopg2.pool import ThreadedConnectionPool

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root, for `db`
from db import connect_kwargs  # noqa: E402

MAX_PATTERN_ROWS = 50
MAX_TEXT = 100  # matches the caps in v_agent_transactions

_pool = None
_pool_lock = threading.Lock()


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    with _pool_lock:
        if _pool is None:
            dbname = os.getenv("AGENT_PGDATABASE") or None
            _pool = ThreadedConnectionPool(0, 4, **connect_kwargs("agent_ro", dbname))
        return _pool


def reset_pool() -> None:
    """Close all pooled connections; the next query reconnects (re-reading AGENT_PGDATABASE)."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.closeall()
            _pool = None


def _query(sql: str, params: tuple = ()) -> list[tuple]:
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = True  # every tool call is a single read; no transaction to hold open
    broken = False
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception:
        broken = conn.closed != 0
        raise
    finally:
        pool.putconn(conn, close=broken)


def _iso(ts) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _num(value):
    return None if value is None else float(value)


# ---------- Tool implementations ----------

def get_account_history(account_id: str) -> dict:
    rows = _query(
        """SELECT home_city, avg_monthly_spend,
                  (now() AT TIME ZONE 'UTC')::date - (account_opened_at AT TIME ZONE 'UTC')::date
           FROM account_profiles WHERE user_id = %s""",
        (account_id,),
    )
    if not rows:
        return {"error": f"No account found with id {account_id}"}
    home_city, avg_spend, age_days = rows[0]
    return {
        "account_id": account_id,
        "home_city": home_city,
        "avg_monthly_spend": _num(avg_spend),
        "account_age_days": age_days,
    }


def get_transaction_pattern(account_id: str) -> dict:
    rows = _query(
        """SELECT transaction_id, event_timestamp, amount, merchant, location_city, total
           FROM (
               SELECT transaction_id, event_timestamp, amount, merchant, location_city,
                      count(*) OVER () AS total
               FROM v_agent_transactions
               WHERE user_id = %s
               ORDER BY event_timestamp DESC, transaction_id DESC
               LIMIT %s
           ) recent
           ORDER BY event_timestamp, transaction_id""",
        (account_id, MAX_PATTERN_ROWS),
    )
    txns = [
        {"txn_id": r[0], "timestamp": _iso(r[1]), "amount": _num(r[2]), "merchant": r[3], "city": r[4]}
        for r in rows
    ]
    result = {"account_id": account_id, "transactions": txns}
    total = rows[0][5] if rows else 0
    if total > len(txns):
        result["truncated"] = True
        result["total_transactions"] = total
    return result


def check_merchant_risk(merchant: str) -> dict:
    merchant = str(merchant)[:MAX_TEXT]
    rows = _query(
        "SELECT risk_level, reason FROM merchant_risk WHERE left(merchant, %s) = %s",
        (MAX_TEXT, merchant),
    )
    if not rows:
        return {"merchant": merchant, "risk_level": "unknown", "reason": "No data on file for this merchant"}
    return {"merchant": merchant, "risk_level": rows[0][0], "reason": rows[0][1]}


def get_geo_consistency(account_id: str, txn_id: str) -> dict:
    """Compares the flagged transaction's city/time to the immediately preceding transaction."""
    rows = _query(
        """SELECT prev_id, prev_ts, prev_city, transaction_id, event_timestamp, location_city
           FROM (
               SELECT transaction_id, event_timestamp, location_city,
                      lag(transaction_id)  OVER w AS prev_id,
                      lag(event_timestamp) OVER w AS prev_ts,
                      lag(location_city)   OVER w AS prev_city
               FROM v_agent_transactions
               WHERE user_id = %s
               WINDOW w AS (ORDER BY event_timestamp, transaction_id)
           ) ordered
           WHERE transaction_id = %s""",
        (account_id, txn_id),
    )
    if not rows:
        return {"error": f"Transaction {txn_id} not found for account {account_id}"}
    prev_id, prev_ts, prev_city, curr_id, curr_ts, curr_city = rows[0]
    if prev_id is None:
        return {"note": "This is the first transaction on record for this account."}

    hours_between = abs((curr_ts - prev_ts).total_seconds()) / 3600
    return {
        "previous_txn": {"txn_id": prev_id, "timestamp": _iso(prev_ts), "city": prev_city},
        "current_txn": {"txn_id": curr_id, "timestamp": _iso(curr_ts), "city": curr_city},
        "hours_between": round(hours_between, 2),
        "same_city": prev_city == curr_city,
    }


# ---------- Schemas (provider-neutral) ----------
#
# This shape (name, description, parameters) is deliberately NOT tied to
# either Anthropic's or Gemini's exact wire format. Each adapter in
# adapters/ reshapes this into whatever its API expects:
#   - Anthropic wants: {"name", "description", "input_schema"}
#   - Gemini wants:    {"type": "function", "name", "description", "parameters"}
# The tool logic below (the actual functions) never changes when you
# swap providers -- only the adapter's reshaping does.

TOOL_SCHEMAS = [
    {
        "name": "get_account_history",
        "description": "Get profile info for an account: home city, average monthly spend, and account age in days. Use this to establish a baseline for what's 'normal' for this account.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "The account ID, e.g. 'acc_001'"}
            },
            "required": ["account_id"],
        },
    },
    {
        "name": "get_transaction_pattern",
        "description": "Get the full recent transaction history for an account, ordered by time. Use this to see spending velocity, typical amounts, and typical merchants.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "The account ID, e.g. 'acc_001'"}
            },
            "required": ["account_id"],
        },
    },
    {
        "name": "check_merchant_risk",
        "description": "Look up whether a merchant name is associated with known fraud patterns.",
        "parameters": {
            "type": "object",
            "properties": {
                "merchant": {"type": "string", "description": "Exact merchant name as it appears in the transaction"}
            },
            "required": ["merchant"],
        },
    },
    {
        "name": "get_geo_consistency",
        "description": "Compare a specific transaction's location and timing to the account's immediately preceding transaction, to check for impossible-travel patterns.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string"},
                "txn_id": {"type": "string", "description": "The transaction ID being investigated"},
            },
            "required": ["account_id", "txn_id"],
        },
    },
]

TOOL_FUNCTIONS = {
    "get_account_history": get_account_history,
    "get_transaction_pattern": get_transaction_pattern,
    "check_merchant_risk": check_merchant_risk,
    "get_geo_consistency": get_geo_consistency,
}
