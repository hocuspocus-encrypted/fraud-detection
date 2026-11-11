"""
Eval harness for the fraud investigation agent.

Runs every case in eval_cases.json through the chosen provider, compares
the verdict against the hand-labeled expectation, prints a report, and
saves the full results (including each agent's tool calls and
explanation) to results/<provider>_<timestamp>.json so runs can be
compared later with compare.py.

Each run rebuilds a throwaway Postgres database (EVAL_DB) from db/migrations,
loads the seed_db.py dataset into it, and points the tools at it, so the
agent is evaluated through the same Postgres tools and agent_ro role it
uses on the live pipeline (PLAN.md Phase 2).

Usage (from the repo root, compose stack up):
    venv\\Scripts\\python.exe agent\\eval.py --provider baseline          # free, offline
    venv\\Scripts\\python.exe agent\\eval.py --provider gemini
    venv\\Scripts\\python.exe agent\\eval.py --provider anthropic
    venv\\Scripts\\python.exe agent\\eval.py --provider gemini --case c13_5 --verbose
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # so imports work no matter where you launch from
sys.path.insert(0, os.path.dirname(HERE))  # repo root, for `db`

from db import connect                            # noqa: E402
from db.migrate import migrate, recreate_database  # noqa: E402
from seed_db import seed_postgres                 # noqa: E402
from adapters.shared import grounding_ok, prompt_version, verdict_for_score  # noqa: E402

EVAL_DB = "fraud_agent_eval"

VERDICTS = ["legitimate", "suspicious", "likely_fraud"]
ACTION_FOR = {
    "legitimate": "approve",
    "suspicious": "hold_for_review",
    "likely_fraud": "block_and_notify_customer",
}


def load_cases(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def lookup_txn(txn_id):
    """Merchant and amount come from the DB so cases can't drift from the data.
    This is the harness reading (as admin), not the agent."""
    conn = connect("admin", EVAL_DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, merchant, amount FROM transactions WHERE transaction_id = %s", (txn_id,)
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(f"txn_id {txn_id!r} not found in database")
    return {"account_id": row[0], "merchant": row[1], "amount": float(row[2])}


def run_case(case, run_investigation, verbose):
    txn = lookup_txn(case["txn_id"])
    if txn["account_id"] != case["account_id"]:
        raise ValueError(f"{case['case_id']}: account_id in cases file does not match the database")

    record = {
        "case_id": case["case_id"],
        "txn_id": case["txn_id"],
        "account_id": case["account_id"],
        "category": case["category"],
        "expected": case["expected_verdict"],
        "acceptable": case.get("acceptable_verdicts", []),
        "predicted": None,
        "risk_score": None,
        "action": None,
        "explanation": None,
        "tools_called": [],
        "n_tool_calls": 0,
        "parse_error": False,
        "error": None,
        "seconds": None,
        "usage": None,
        "confidence": None,
        "grounding_ok": None,
    }

    start = time.time()
    try:
        result = run_investigation(
            txn["account_id"], case["txn_id"], txn["merchant"], txn["amount"], verbose=verbose
        )
        v = result["verdict"]
        record["usage"] = result.get("usage")
        record["tools_called"] = [s["tool"] for s in result["trace"] if s["type"] == "tool_call"]
        record["n_tool_calls"] = len(record["tools_called"])

        if v.get("parse_error"):
            record["parse_error"] = True
            record["explanation"] = v.get("raw_output")
        elif "error" in v:
            record["error"] = v["error"]
        else:
            pred = str(v.get("verdict", "")).strip().lower()
            record["predicted"] = pred if pred in VERDICTS else "invalid"
            record["risk_score"] = v.get("risk_score")
            record["action"] = v.get("recommended_action")
            record["explanation"] = v.get("explanation")
            record["confidence"] = v.get("confidence")
            record["grounding_ok"] = grounding_ok(v, result["trace"], prompt_version())
    except Exception as e:  # keep the run going; a crash on one case shouldn't lose the rest
        record["error"] = f"{type(e).__name__}: {str(e)[:300]}"
    record["seconds"] = round(time.time() - start, 2)

    p = record["predicted"]
    record["correct_strict"] = p == record["expected"]
    record["correct_lenient"] = p == record["expected"] or p in record["acceptable"]
    record["action_consistent"] = (
        None if p not in VERDICTS else record["action"] == ACTION_FOR[p]
    )
    # The worker stores the band of the risk_score, not the model's label (verdict_from_score).
    score = record["risk_score"]
    from_score = verdict_for_score(score) if isinstance(score, (int, float)) and p in VERDICTS else None
    record["from_score"] = from_score
    record["correct_strict_from_score"] = from_score == record["expected"]
    record["correct_lenient_from_score"] = from_score == record["expected"] or from_score in record["acceptable"]
    return record


def summarize(results, provider):
    n = len(results)
    strict = sum(r["correct_strict"] for r in results)
    lenient = sum(r["correct_lenient"] for r in results)

    print("\n" + "=" * 78)
    print(f"RESULTS  provider={provider}  prompt={prompt_version()}  cases={n}")
    print("=" * 78)

    print(f"\n{'case':<8} {'category':<28} {'expected':<13} {'predicted':<13} {'score':>5}  result")
    print("-" * 78)
    for r in results:
        if r["error"]:
            shown = "ERROR"
        elif r["parse_error"]:
            shown = "PARSE_ERR"
        else:
            shown = r["predicted"] or "-"
        if r["correct_strict"]:
            mark = "OK"
        elif r["correct_lenient"]:
            mark = "ok (acceptable)"
        else:
            mark = "MISS"
        score = "" if r["risk_score"] is None else str(r["risk_score"])
        print(f"{r['case_id']:<8} {r['category']:<28} {r['expected']:<13} {shown:<13} {score:>5}  {mark}")

    print(f"\nStrict accuracy  (exact match only):          {strict}/{n}  ({100 * strict / n:.0f}%)")
    print(f"Lenient accuracy (exact or acceptable):       {lenient}/{n}  ({100 * lenient / n:.0f}%)")
    strict_s = sum(r["correct_strict_from_score"] for r in results)
    lenient_s = sum(r["correct_lenient_from_score"] for r in results)
    print(f"Label from score (what the worker stores):    strict {strict_s}/{n} ({100 * strict_s / n:.0f}%)  "
          f"lenient {lenient_s}/{n} ({100 * lenient_s / n:.0f}%)")
    checked = [r for r in results if r["grounding_ok"] is not None]
    if checked:
        grounded = sum(r["grounding_ok"] for r in checked)
        print(f"Evidence grounded in the trace (v4):          {grounded}/{len(checked)}")
        print("Confidence given: " + ", ".join(
            f"{c}={sum(r['confidence'] == c for r in results)}" for c in ("low", "medium", "high", None)))

    # Confusion matrix
    cols = VERDICTS + ["other"]
    print("\nConfusion matrix (rows = expected, columns = predicted):")
    print(f"{'':<14}" + "".join(f"{c:>14}" for c in cols))
    for exp in VERDICTS:
        row = [0, 0, 0, 0]
        for r in results:
            if r["expected"] != exp:
                continue
            p = r["predicted"]
            row[VERDICTS.index(p) if p in VERDICTS else 3] += 1
        print(f"{exp:<14}" + "".join(f"{x:>14}" for x in row))

    # The errors that matter most, called out separately
    missed_fraud = [r["case_id"] for r in results if r["expected"] == "likely_fraud" and r["predicted"] == "legitimate"]
    blocked_good = [r["case_id"] for r in results if r["expected"] == "legitimate" and r["predicted"] == "likely_fraud"]
    over_flagged = [r["case_id"] for r in results
                    if r["expected"] == "legitimate" and r["predicted"] in ("suspicious", "likely_fraud")]
    print("\nCosts of being wrong are not symmetric:")
    print(f"  Missed fraud   (expected likely_fraud, approved):    {len(missed_fraud)}  {missed_fraud or ''}")
    print(f"  Blocked a good customer (legit -> likely_fraud):     {len(blocked_good)}  {blocked_good or ''}")
    print(f"  Any over-flag of a legit case (incl. 'suspicious'):  {len(over_flagged)}  {over_flagged or ''}")

    # Behavioral checks
    no_tools = [r["case_id"] for r in results if r["n_tool_calls"] == 0 and not r["error"]]
    errors = [r["case_id"] for r in results if r["error"]]
    parse_errs = [r["case_id"] for r in results if r["parse_error"]]
    bad_action = [r["case_id"] for r in results if r["action_consistent"] is False]
    ok_runs = [r for r in results if not r["error"]]
    avg_calls = sum(r["n_tool_calls"] for r in ok_runs) / len(ok_runs) if ok_runs else 0
    avg_secs = sum(r["seconds"] for r in results) / n
    print("\nBehavior checks:")
    print(f"  Verdict with zero tool calls:                {len(no_tools)}  {no_tools or ''}")
    print(f"  Unparseable output / run errors:             {len(parse_errs)} / {len(errors)}  {(parse_errs + errors) or ''}")
    print(f"  Action contradicts the agent's own verdict:  {len(bad_action)}  {bad_action or ''}")
    print(f"  Avg tool calls per case: {avg_calls:.1f}   Avg seconds per case: {avg_secs:.1f}")

    # Cost: summed over every model turn of each investigation (PLAN.md: set the
    # worker's hourly budget from this before running it on the stream)
    used = [r["usage"] for r in results if r.get("usage")]
    if used:
        def avg(field):
            return sum(u.get(field, 0) for u in used) / len(used)
        print(f"\nTokens per case (avg over {len(used)}): input {avg('total_input_tokens'):,.0f}  "
              f"output {avg('total_output_tokens'):,.0f}  thinking {avg('total_thought_tokens'):,.0f}  "
              f"total {avg('total_tokens'):,.0f}   model turns {avg('turns'):.1f}")
        worst = max(used, key=lambda u: u.get("total_tokens", 0))
        print(f"  Largest single case: {worst.get('total_tokens', 0):,} tokens")

    # Show the reasoning for every miss -- this is where you learn something
    misses = [r for r in results if not r["correct_lenient"]]
    if misses:
        print("\nMisses (read these -- they are the interesting part):")
        for r in misses:
            print(f"\n  [{r['case_id']}] expected {r['expected']}, got {r['predicted'] or r['error']}")
            if r["explanation"]:
                print(f"    agent said: {r['explanation'][:400]}")
    print()


def main():
    ap = argparse.ArgumentParser(description="Evaluate the fraud agent against labeled cases.")
    ap.add_argument("--provider", default=os.environ.get("MODEL_PROVIDER", "baseline"),
                    choices=["anthropic", "gemini", "ollama", "baseline"])
    ap.add_argument("--cases", default=os.path.join(HERE, "eval_cases.json"))
    ap.add_argument("--case", action="append", help="run only this case_id (repeatable)")
    ap.add_argument("--verbose", action="store_true", help="print each tool call as it happens")
    ap.add_argument("--sleep", type=float, default=0.0, help="seconds to pause between cases (rate limits)")
    ap.add_argument("--out-dir", default=os.path.join(HERE, "results"))
    args = ap.parse_args()

    # fresh, deterministic DB for every run
    recreate_database(EVAL_DB)
    migrate(EVAL_DB)
    seed_postgres(EVAL_DB)

    os.environ["MODEL_PROVIDER"] = args.provider
    os.environ["AGENT_PGDATABASE"] = EVAL_DB
    from orchestrator import run_investigation  # import after env is set

    cases = load_cases(args.cases)
    if args.case:
        wanted = set(args.case)
        cases = [c for c in cases if c["case_id"] in wanted]
        if not cases:
            sys.exit(f"No cases matched {sorted(wanted)}")

    results = []
    for i, case in enumerate(cases, 1):
        print(f"[{i}/{len(cases)}] {case['case_id']} ({case['category']}) ...", flush=True)
        results.append(run_case(case, run_investigation, args.verbose))
        if args.sleep and i < len(cases):
            time.sleep(args.sleep)

    summarize(results, args.provider)

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(args.out_dir, f"{args.provider}_{prompt_version()}_{stamp}.json")
    from orchestrator import current_model
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "meta": {"provider": args.provider, "model": current_model()[1], "prompt_version": prompt_version(),
                     "timestamp": stamp, "n_cases": len(results)},
            "results": results,
        }, f, indent=2)
    print(f"Saved full results to {path}")


if __name__ == "__main__":
    main()