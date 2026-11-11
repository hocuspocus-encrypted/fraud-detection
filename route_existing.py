"""
route_existing.py

Backfills routing (docs/PLAN_HITL.md Phase H3) on investigations that finished before
the worker routed its own results. For each done, failed or dead row with no
review_status, it:

  1. fills rule_hits and signals with compute_incident_signals() (write-once, as in H1)
  2. runs the rule baseline if the row has no baseline result yet. For rows from before
     H1 this reads the data as it is now, not as the worker would have seen it.
  3. routes the row with routing.route() and stores the result

Steps 2-3 write in one transaction per row. Rows that are already routed (or reviewed)
and rows still pending or running are never touched, so it is safe to re-run.

Usage (from the repo root):
    venv\\Scripts\\python.exe route_existing.py
    venv\\Scripts\\python.exe route_existing.py --dbname some_other_db
"""
import argparse
import json
import os
import sys
from collections import Counter

from db import ROOT, connect
from investigation_worker import write_route
from routing import RoutingConfig

sys.path.insert(0, str(ROOT / "agent"))


def default_baseline():
    from adapters import baseline_adapter
    return baseline_adapter.run_investigation


def backfill(dbname: str | None = None, config: RoutingConfig | None = None, baseline=None) -> dict:
    """Route every unrouted finished row; returns a count per review_status written."""
    config = config or RoutingConfig.from_env()
    baseline = baseline or default_baseline()
    counts = Counter()
    conn = connect("worker", dbname)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT id FROM investigations
                           WHERE review_status IS NULL AND status IN ('done', 'failed', 'dead')
                           ORDER BY id""")
            ids = [r[0] for r in cur.fetchall()]

        for iid in ids:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT compute_incident_signals(%s)", (iid,))
                cur.execute("""SELECT i.user_id, i.primary_txn_id, i.baseline_verdict, t.merchant, t.amount
                               FROM investigations i
                               LEFT JOIN transactions t ON t.transaction_id = i.primary_txn_id
                               WHERE i.id = %s""", (iid,))
                user_id, txn_id, b_verdict, merchant, amount = cur.fetchone()

            result = None
            if b_verdict is None and amount is not None:
                try:
                    result = baseline(user_id, txn_id, merchant, float(amount), verbose=False)["verdict"]
                except Exception as e:  # routed without a baseline; done rows then go to review
                    print(json.dumps({"event": "baseline_failed", "investigation_id": iid,
                                      "error": f"{type(e).__name__}: {e}"[:500]}), flush=True)

            with conn, conn.cursor() as cur:
                if result is not None:
                    cur.execute("""UPDATE investigations
                                   SET baseline_score = %s, baseline_verdict = %s, baseline_action = %s
                                   WHERE id = %s AND baseline_verdict IS NULL""",
                                (result.get("risk_score"), result.get("verdict"),
                                 result.get("recommended_action"), iid))
                status = write_route(cur, iid, config)
            if status:
                counts[status] += 1
    finally:
        conn.close()
    return dict(counts)


def main():
    parser = argparse.ArgumentParser(description="Route finished investigations that have no review_status.")
    parser.add_argument("--dbname", default=None, help="Database (default: PGDATABASE from .env).")
    args = parser.parse_args()
    if args.dbname:
        os.environ.setdefault("AGENT_PGDATABASE", args.dbname)  # the baseline's tools read the same database
    counts = backfill(args.dbname)
    print(json.dumps({"event": "routed", "total": sum(counts.values()), **counts}))


if __name__ == "__main__":
    main()
