"""
incident_eval.py

Labeled eval on pipeline-shaped incidents, much larger than eval_cases.json
and scored against the generator's ground truth.

How cases are built (deterministic, no Kafka or Spark):
  - the Phase 4 generator produces a stream on a fake clock: the same 5000
    customers as live (seed 42), but a different transaction stream per split,
    so dev and test share no transactions
  - simulate_flags.find_incidents() applies the Spark windows/thresholds and the
    enqueuer's merging (it matched the live run within ~2%)
  - the first WARMUP_HOURS only build history; incidents start after that
  - each incident's flagged windows are inserted into anomaly_flags with their
    final totals, so the worker's compute_incident_signals() fills rule_hits
  - each case is investigated at incident end + 5 min (the enqueuer's settle
    delay) against a database holding only the transactions up to that moment,
    like live, through the real worker (claim, investigate, one retry for
    unparseable output, transient retries)

Scoring, fixed on 2026-10-28 before any run:
    incident contains labeled fraud        expected likely_fraud, also acceptable suspicious
    legit_travel / big_purchase / new_merchant   expected legitimate, also acceptable suspicious
    ordinary behavior (no label)           expected legitimate
Reported: strict/lenient accuracy, fraud caught (not cleared), legitimate
cleared, good customers blocked, flip rate across runs, and accuracy if the
label were derived from risk_score with the v3 bands. Tune on "dev"; run
"test" once at the end.

Usage (from the repo root; provider and prompt from .env or the environment):
    venv\\Scripts\\python.exe agent\\incident_eval.py --split dev --runs 3
    $env:AGENT_PROMPT_VERSION = "v3"; venv\\Scripts\\python.exe agent\\incident_eval.py --split dev --runs 3
"""
import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))  # repo root

import generate_transactions as gt  # noqa: E402
from adapters.shared import prompt_version, verdict_for_score  # noqa: E402
from db import connect  # noqa: E402
from db.migrate import migrate, recreate_database  # noqa: E402
from seed_reference_data import seed_reference_data  # noqa: E402
from simulate_flags import SIM_START, WINDOW_SECONDS, find_incidents, flagged_windows, generate_stream  # noqa: E402

EVAL_DB = "fraud_agent_incident_eval"
SPLITS = {"dev": {"stream_seed": 1001, "hours": 1.5}, "test": {"stream_seed": 2002, "hours": 2.5}}
WARMUP_HOURS = 2
WARMUP_END = SIM_START + timedelta(hours=WARMUP_HOURS)
SETTLE = timedelta(minutes=5)
VERDICTS = ("legitimate", "suspicious", "likely_fraud")


def expected_verdicts(kinds: set[str]) -> tuple[str, list[str]]:
    if kinds & set(gt.FRAUD_KINDS):
        return "likely_fraud", ["suspicious"]
    if kinds:
        return "legitimate", ["suspicious"]
    return "legitimate", []


def build_split(split: str) -> tuple[list[dict], list[tuple]]:
    """(cases in investigation order, the full stream of (record, label))."""
    cfg = SPLITS[split]
    total_hours = WARMUP_HOURS + cfg["hours"]
    _, events = generate_stream(seed=gt.DEFAULT_SEED, n_users=gt.DEFAULT_USERS, hours=total_hours,
                                stream_seed=cfg["stream_seed"])
    stream_end = SIM_START + timedelta(hours=total_hours)
    windows = flagged_windows(events)
    by_user = defaultdict(list)
    for record, label in events:
        by_user[record["user_id"]].append((datetime.fromisoformat(record["timestamp"]), record, label))

    cases = []
    for user, s, e in find_incidents(events):
        start, end = datetime.fromtimestamp(s, timezone.utc), datetime.fromtimestamp(e, timezone.utc)
        if start < WARMUP_END or end + SETTLE > stream_end:
            continue
        inside = sorted(((t, r, lb) for t, r, lb in by_user[user] if start <= t < end),
                        key=lambda x: (x[0], x[1]["transaction_id"]))
        kinds = {lb["label_type"] for _, _, lb in inside if lb}
        primary = max((r for _, r, _ in inside),
                      key=lambda r: (r["amount"], datetime.fromisoformat(r["timestamp"]), r["transaction_id"]))
        expected, acceptable = expected_verdicts(kinds)
        fraud = any(r["is_fraud"] for _, r, _ in inside)
        # The flagged windows the incident was merged from, with their final totals, so
        # compute_incident_signals() sees the same anomaly_flags rows as live.
        flags = [{"window_start": datetime.fromtimestamp(ws, timezone.utc).isoformat(),
                  "window_end": datetime.fromtimestamp(ws + WINDOW_SECONDS, timezone.utc).isoformat(),
                  "txn_count": count, "total_amount": total}
                 for ws, count, total in windows[user] if s <= ws and ws + WINDOW_SECONDS <= e]
        cases.append({
            "case_id": f"{split}-{len(cases):03d}",
            "user_id": user,
            "incident_start": start.isoformat(), "incident_end": end.isoformat(),
            "investigate_at": (end + SETTLE).isoformat(),
            "txn_ids": [r["transaction_id"] for _, r, _ in inside],
            "txn_timestamps": [r["timestamp"] for _, r, _ in inside],
            "primary_txn_id": primary["transaction_id"],
            "flags": flags,
            "kinds": sorted(kinds),
            "category": "fraud" if fraud else ("legit_oddity" if kinds else "ordinary"),
            "fraud": fraud,
            "expected": expected, "acceptable": acceptable,
        })
    return cases, events


def _insert_transactions(conn, records: list[dict]) -> None:
    with conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency, merchant,
                                         merchant_category, card_type, card_last4, location_city,
                                         location_country, is_fraud, ingested_at)
               VALUES (%(transaction_id)s, %(timestamp)s, %(user_id)s, %(amount)s, %(currency)s, %(merchant)s,
                       %(merchant_category)s, %(card_type)s, %(card_last4)s, %(location_city)s,
                       %(location_country)s, %(is_fraud)s, %(timestamp)s::timestamptz + interval '3 seconds')""",
            records,
        )


def run_split(split: str, investigate=None, provider: str | None = None, model: str | None = None,
              limit: int | None = None, progress: bool = False) -> list[dict]:
    """One run over a split through the real worker. Returns one result per case, in order."""
    from investigation_worker import Worker, WorkerConfig  # imported late: needs agent/ on sys.path

    os.environ["AGENT_PGDATABASE"] = EVAL_DB
    if "tools" in sys.modules:
        sys.modules["tools"].reset_pool()
    recreate_database(EVAL_DB)
    migrate(EVAL_DB)
    seed_reference_data(dbname=EVAL_DB)

    cases, events = build_split(split)
    cases = cases[:limit] if limit else cases
    worker = Worker(
        WorkerConfig(concurrency=1, hourly_budget=10**6, max_attempts=3, backoff_seconds=0,
                     timeout_seconds=float(os.getenv("WORKER_TIMEOUT_SECONDS", "180")), stuck_after_seconds=3600),
        dbname=EVAL_DB, investigate=investigate, provider=provider, model=model,
    )
    conn = connect("admin", EVAL_DB)
    results, loaded = [], 0
    try:
        for i, case in enumerate(cases, 1):
            cutoff = datetime.fromisoformat(case["investigate_at"])
            batch = []
            while loaded < len(events) and datetime.fromisoformat(events[loaded][0]["timestamp"]) <= cutoff:
                batch.append(events[loaded][0])
                loaded += 1
            if batch:
                _insert_transactions(conn, batch)
            with conn, conn.cursor() as cur:
                flag_ids = []
                for f in case["flags"]:
                    cur.execute(
                        """INSERT INTO anomaly_flags (user_id, window_start, window_end, txn_count, total_amount)
                           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                        (case["user_id"], f["window_start"], f["window_end"], f["txn_count"], f["total_amount"]),
                    )
                    flag_ids.append(cur.fetchone()[0])
                cur.execute(
                    """INSERT INTO investigations (user_id, incident_start, incident_end, flag_ids, txn_ids, primary_txn_id)
                       VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                    (case["user_id"], case["incident_start"], case["incident_end"], flag_ids, case["txn_ids"],
                     case["primary_txn_id"]),
                )
                iid = cur.fetchone()[0]

            started = time.monotonic()
            for _ in range(worker.cfg.max_attempts):
                for row in worker.claim(1):
                    worker.process(row)
                with conn, conn.cursor() as cur:
                    cur.execute("SELECT status FROM investigations WHERE id = %s", (iid,))
                    if cur.fetchone()[0] not in ("pending", "running"):
                        break
            seconds = round(time.monotonic() - started, 1)

            with conn, conn.cursor() as cur:
                cur.execute("""SELECT status, verdict, risk_score, recommended_action, error, prompt_version,
                                      provider, model, trace, usage, attempts, confidence, grounding_ok, rule_hits
                               FROM investigations WHERE id = %s""", (iid,))
                (status, verdict, risk_score, action, error, pv, prov, mdl, trace, usage, attempts,
                 confidence, grounding, rule_hits) = cur.fetchone()
            steps = trace or []
            results.append({
                "case_id": case["case_id"], "status": status, "verdict": verdict,
                "risk_score": None if risk_score is None else float(risk_score), "action": action,
                "error": error, "prompt_version": pv, "provider": prov, "model": mdl, "attempts": attempts,
                "confidence": confidence, "grounding_ok": grounding, "rule_hits": rule_hits,
                "seconds": seconds,
                "tool_calls": sum(1 for s in steps if s.get("type") == "tool_call" and not s.get("prefetched")),
                "prefetched": sum(1 for s in steps if s.get("prefetched")),
                "tools_seen": sorted({s.get("tool") for s in steps if s.get("type") == "tool_call"}),
                "tokens": (usage or {}).get("total_tokens"),
            })
            if progress:
                r = results[-1]
                print(f"[{i}/{len(cases)}] {case['case_id']} {case['category']:<12} expected {case['expected']:<12} "
                      f"-> {r['verdict'] or r['status']:<12} score {r['risk_score']}  {seconds}s", flush=True)
    finally:
        conn.close()
    return results


def score(runs: list[list[dict]], cases: list[dict]) -> dict:
    by_case = {c["case_id"]: c for c in cases}
    pairs = [(by_case[r["case_id"]], r) for run in runs for r in run]

    def verdict(r):
        return r["verdict"] if r["status"] == "done" else None

    def from_score(r):
        return verdict_for_score(r["risk_score"]) if r["status"] == "done" and r["risk_score"] is not None else None

    def rate(items, ok):
        items = list(items)
        return sum(1 for x in items if ok(x)) / len(items) if items else None

    fraud = [(c, r) for c, r in pairs if c["fraud"]]
    legit = [(c, r) for c, r in pairs if not c["fraud"]]
    verdicts_per_case = defaultdict(set)
    for c, r in pairs:
        verdicts_per_case[c["case_id"]].add(verdict(r))
    return {
        "cases": len(cases), "runs": len(runs),
        "strict": rate(pairs, lambda p: verdict(p[1]) == p[0]["expected"]),
        "lenient": rate(pairs, lambda p: verdict(p[1]) == p[0]["expected"] or verdict(p[1]) in p[0]["acceptable"]),
        "fraud_caught": rate(fraud, lambda p: verdict(p[1]) in ("suspicious", "likely_fraud")),
        "fraud_likely": rate(fraud, lambda p: verdict(p[1]) == "likely_fraud"),
        "legit_cleared": rate(legit, lambda p: verdict(p[1]) == "legitimate"),
        "good_blocked": rate(legit, lambda p: verdict(p[1]) == "likely_fraud"),
        "flip_rate": (sum(1 for v in verdicts_per_case.values() if len(v) > 1) / len(verdicts_per_case)
                      if len(runs) > 1 else 0.0),
        "strict_from_score": rate(pairs, lambda p: from_score(p[1]) == p[0]["expected"]),
        "lenient_from_score": rate(pairs, lambda p: from_score(p[1]) == p[0]["expected"]
                                   or from_score(p[1]) in p[0]["acceptable"]),
        "not_done": sum(1 for _, r in pairs if r["status"] != "done"),
        "avg_seconds": sum(r.get("seconds", 0) for _, r in pairs) / len(pairs) if pairs else None,
        "avg_model_tool_calls": sum(r.get("tool_calls", 0) for _, r in pairs) / len(pairs) if pairs else None,
        "all_four_checks": rate(pairs, lambda p: len(p[1].get("tools_seen", [])) == 4),
        "grounded": rate([p for p in pairs if p[1].get("grounding_ok") is not None],
                         lambda p: p[1]["grounding_ok"]),
        "confidence_counts": dict(Counter(r.get("confidence") for _, r in pairs if r["status"] == "done")),
        "verdict_counts": dict(Counter(f"{c.get('category', '?')}->{verdict(r)}" for c, r in pairs)),
    }


def main():
    parser = argparse.ArgumentParser(description="Labeled incident eval through the real worker.")
    parser.add_argument("--split", choices=list(SPLITS), default="dev")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--limit", type=int, default=None, help="Only the first N cases (smoke test).")
    parser.add_argument("--out-dir", default=str(HERE / "results"))
    args = parser.parse_args()

    import orchestrator
    provider, model = orchestrator.current_model()
    version = prompt_version()
    cases, _ = build_split(args.split)
    cases = cases[:args.limit] if args.limit else cases
    print(f"split {args.split}: {len(cases)} cases ({Counter(c['category'] for c in cases)}); "
          f"provider {provider}, model {model}, prompt {version}, runs {args.runs}", flush=True)

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path(args.out_dir) / f"incident_{args.split}_{provider}_{version}_{stamp}.json"

    runs = []
    for n in range(args.runs):
        print(f"--- run {n + 1}/{args.runs}", flush=True)
        runs.append(run_split(args.split, limit=args.limit, progress=True))
        # Saved after every run, so an interrupted eval (e.g. the laptop sleeping) keeps finished runs.
        metrics = score(runs, cases)
        path.write_text(json.dumps({"meta": {"split": args.split, "provider": provider, "model": model,
                                             "prompt_version": version, "runs_planned": args.runs,
                                             "runs_completed": len(runs), "limit": args.limit},
                                    "metrics": metrics, "cases": cases, "runs": runs}, indent=2, default=str),
                        encoding="utf-8")
        print(f"saved {path} ({len(runs)}/{args.runs} runs)", flush=True)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
