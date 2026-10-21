"""
REFERENCE ONLY: the original SQLite tool bodies from the fraud-agent
scaffold, kept unchanged so tests/test_phase2_tools.py can check that the
Postgres versions in tools.py give the same answers. The agent never
imports this module.
"""
import sqlite3
import os
import math
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "fraud_demo.db")


def _conn():
    return sqlite3.connect(DB_PATH)


# ---------- Tool implementations ----------

def get_account_history(account_id: str) -> dict:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT home_city, avg_monthly_spend, account_age_days FROM accounts WHERE account_id = ?",
        (account_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"error": f"No account found with id {account_id}"}
    return {
        "account_id": account_id,
        "home_city": row[0],
        "avg_monthly_spend": row[1],
        "account_age_days": row[2],
    }


def get_transaction_pattern(account_id: str) -> dict:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT txn_id, timestamp, amount, merchant, city
           FROM transactions WHERE account_id = ? ORDER BY timestamp""",
        (account_id,),
    )
    rows = cur.fetchall()
    conn.close()
    txns = [
        {"txn_id": r[0], "timestamp": r[1], "amount": r[2], "merchant": r[3], "city": r[4]}
        for r in rows
    ]
    return {"account_id": account_id, "transactions": txns}


def check_merchant_risk(merchant: str) -> dict:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT risk_level, reason FROM merchant_risk WHERE merchant = ?", (merchant,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"merchant": merchant, "risk_level": "unknown", "reason": "No data on file for this merchant"}
    return {"merchant": merchant, "risk_level": row[0], "reason": row[1]}


def get_geo_consistency(account_id: str, txn_id: str) -> dict:
    """Compares the flagged transaction's city/time to the immediately preceding transaction."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """SELECT txn_id, timestamp, city FROM transactions
           WHERE account_id = ? ORDER BY timestamp""",
        (account_id,),
    )
    rows = cur.fetchall()
    conn.close()

    idx = next((i for i, r in enumerate(rows) if r[0] == txn_id), None)
    if idx is None:
        return {"error": f"Transaction {txn_id} not found for account {account_id}"}
    if idx == 0:
        return {"note": "This is the first transaction on record for this account."}

    prev = rows[idx - 1]
    curr = rows[idx]
    t1 = datetime.fromisoformat(prev[1])
    t2 = datetime.fromisoformat(curr[1])
    hours_between = abs((t2 - t1).total_seconds()) / 3600

    return {
        "previous_txn": {"txn_id": prev[0], "timestamp": prev[1], "city": prev[2]},
        "current_txn": {"txn_id": curr[0], "timestamp": curr[1], "city": curr[2]},
        "hours_between": round(hours_between, 2),
        "same_city": prev[2] == curr[2],
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