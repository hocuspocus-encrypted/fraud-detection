"""
investigation_worker.py

Runs the fraud investigation agent on incidents queued by
investigation_enqueuer.py and writes each verdict, explanation, evidence and
tool trace back to the same `investigations` row (PLAN.md Phase 3). It runs
as the `worker` DB role; the agent's own tools use `agent_ro`. It never
touches Spark or Kafka, so a slow or stopped worker cannot stall the stream.

Loop: claim up to WORKER_CONCURRENCY pending rows (FOR UPDATE SKIP LOCKED),
mark them running, investigate each incident's primary transaction with the
incident as context, then write the result.

  - Transient errors (rate limit, 5xx, timeouts, connection errors) put the row
    back to 'pending' with exponential backoff; after WORKER_MAX_ATTEMPTS it is 'dead'.
  - Permanent errors (unparseable output after one retry, tool failure, max
    iterations) mark it 'failed'.
  - A row left 'running' longer than WORKER_STUCK_AFTER_SECONDS (worker crash)
    is reclaimed. Results are written only if the row is still on the same
    attempt, so a late result from a reclaimed attempt is discarded.
  - Every claim is recorded in investigation_attempts; WORKER_HOURLY_BUDGET caps
    attempts started per rolling hour, across restarts.
  - Kill switch: WORKER_ENABLED=false stops new claims (in-flight ones finish).
    It is re-read from .env every loop, so it takes effect without a restart.

Usage (from the repo root):
    venv\\Scripts\\python.exe investigation_worker.py               (run forever)
    venv\\Scripts\\python.exe investigation_worker.py --until-idle  (exit when the queue is drained)

Logs are JSON lines keyed by investigation_id and primary_txn_id.
"""
import argparse
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass

import httpx
import psycopg2
from dotenv import dotenv_values
from psycopg2.extras import Json

from db import ROOT, connect

sys.path.insert(0, str(ROOT / "agent"))
from adapters.shared import ACTION_FOR, prompt_version, verdict_for_score  # noqa: E402

VERDICTS = {"legitimate", "suspicious", "likely_fraud"}
ACTIONS = {"approve", "hold_for_review", "block_and_notify_customer"}
MAX_ERROR_CHARS = 1000

log = logging.getLogger("worker")


def log_event(event: str, **fields) -> None:
    log.info(json.dumps({"event": event, **fields}, default=str))


@dataclass
class WorkerConfig:
    concurrency: int = 2
    hourly_budget: int = 20
    timeout_seconds: float = 180.0
    max_attempts: int = 3
    backoff_seconds: float = 30.0
    stuck_after_seconds: float = 600.0
    poll_seconds: float = 5.0

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        values = {}
        for name, field in cls.__dataclass_fields__.items():
            raw = os.getenv(f"WORKER_{name.upper()}")
            if raw not in (None, ""):
                values[name] = field.type(raw) if callable(field.type) else type(field.default)(raw)
        return cls(**values)


def is_enabled() -> bool:
    """Kill switch. Off if WORKER_ENABLED is false in the process environment or in
    .env as it is on disk right now. Turning it off in .env takes effect on the next
    loop; a worker started with it off needs a restart to turn back on."""
    for value in (os.environ.get("WORKER_ENABLED"), dotenv_values(ROOT / ".env").get("WORKER_ENABLED")):
        if value is not None and value.strip().lower() in ("false", "0", "no", "off"):
            return False
    return True


class InvestigationTimeout(Exception):
    pass


def sum_usage(*usages) -> dict | None:
    """Add per-call token counts (each retry of a model call is billed too)."""
    present = [u for u in usages if u]
    if not present:
        return None
    total = {}
    for u in present:
        for k, v in u.items():
            if isinstance(v, (int, float)):
                total[k] = total.get(k, 0) + v
    return total


def is_transient(exc: BaseException) -> bool:
    """Rate limits, 5xx, timeouts and connection errors are worth retrying; anything
    else (bad arguments, tool failures, auth errors) will fail the same way again."""
    if isinstance(exc, (InvestigationTimeout, TimeoutError, ConnectionError)):
        return True
    # httpx (Ollama adapter; also under the Gemini/Anthropic SDKs): server down,
    # connection reset, timeouts. ConnectError's name doesn't match the checks below.
    if isinstance(exc, (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError)):
        return True
    for attr in ("status_code", "code", "status"):
        status = getattr(exc, attr, None)
        if isinstance(status, int):
            return status in (408, 429) or status >= 500
    name = type(exc).__name__
    return "Timeout" in name or "Connection" in name or "RateLimit" in name


def valid_verdict(v, prompt_version: str = "v2") -> bool:
    """Well-formed verdict. Under prompt v3 the label must also match the band of its own
    risk_score and the action must match the label; a contradiction counts as unparseable."""
    if not isinstance(v, dict):
        return False
    score = v.get("risk_score")
    verdict = str(v.get("verdict", "")).strip().lower()
    ok = (
        verdict in VERDICTS
        and v.get("recommended_action") in ACTIONS
        and isinstance(score, (int, float)) and not isinstance(score, bool) and 0 <= score <= 100
    )
    if ok and prompt_version == "v3":
        ok = verdict == verdict_for_score(score) and v["recommended_action"] == ACTION_FOR[verdict]
    return ok


def verdict_from_score(v: dict) -> tuple[dict, dict]:
    """Deterministic code decides, the model interprets: the stored verdict and action are
    the band of the model's risk_score (0-30 legitimate, 31-70 suspicious, 71-100
    likely_fraud). On the dev split qwen3:4b-instruct's scores separated the classes well
    but it labelled 92% of non-fraud 'suspicious'. Returns the verdict to store and a trace
    entry that keeps the model's own label for audit."""
    verdict = verdict_for_score(v["risk_score"])
    entry = {"type": "verdict_from_score", "risk_score": v["risk_score"],
             "model_verdict": str(v["verdict"]).strip().lower(), "model_action": v["recommended_action"],
             "verdict": verdict, "recommended_action": ACTION_FOR[verdict]}
    return dict(v, verdict=verdict, recommended_action=ACTION_FOR[verdict]), entry


class Worker:
    def __init__(self, config: WorkerConfig, dbname: str | None = None,
                 investigate=None, provider: str | None = None, model: str | None = None,
                 baseline=None):
        if config.stuck_after_seconds <= config.timeout_seconds:
            raise ValueError("stuck_after_seconds must exceed timeout_seconds, "
                             "or live investigations would be reclaimed as stuck")
        self.cfg = config
        self.dbname = dbname
        if investigate is None:
            import orchestrator
            investigate = orchestrator.run_investigation
            provider, model = orchestrator.current_model()
            if baseline is None:
                from adapters import baseline_adapter
                baseline = baseline_adapter.run_investigation
        self.investigate = investigate
        # Rule scorer run on the same incident before the agent, as a second opinion
        # (docs/PLAN_HITL.md H1). None skips it; tests that inject investigate pass their own.
        self.baseline = baseline
        self.provider, self.model = provider, model
        self.prompt_version = prompt_version()
        self._conn = None
        self._budget_exhausted = False

    # ---------- queue ----------

    def _claim_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = connect("worker", self.dbname)
        return self._conn

    def claim(self, n: int) -> list[dict]:
        """Reap stuck rows, then claim up to n claimable rows within the hourly budget."""
        if n <= 0:
            return []
        stuck = f"{self.cfg.stuck_after_seconds} seconds"
        conn = self._claim_conn()
        with conn, conn.cursor() as cur:
            # One claimer at a time, so the budget check and the claim can't interleave.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('investigation_worker_claim'))")

            cur.execute(
                """UPDATE investigation_attempts a
                   SET outcome = 'abandoned', finished_at = now(),
                       error = 'worker stopped responding; row reclaimed'
                   FROM investigations i
                   WHERE a.investigation_id = i.id AND a.attempt = i.attempts AND a.finished_at IS NULL
                     AND i.status = 'running' AND i.claimed_at < now() - %s::interval""",
                (stuck,),
            )
            cur.execute(
                """UPDATE investigations
                   SET status = 'dead', finished_at = now(),
                       error = format('stuck in running after %%s attempt(s); giving up', attempts)
                   WHERE status = 'running' AND claimed_at < now() - %s::interval AND attempts >= %s
                   RETURNING id, primary_txn_id""",
                (stuck, self.cfg.max_attempts),
            )
            for iid, txn in cur.fetchall():
                log_event("dead", investigation_id=iid, primary_txn_id=txn, reason="stuck")

            cur.execute("SELECT count(*) FROM investigation_attempts WHERE started_at > now() - interval '1 hour'")
            remaining = self.cfg.hourly_budget - cur.fetchone()[0]
            if remaining <= 0:
                if not self._budget_exhausted:
                    log_event("budget_exhausted", hourly_budget=self.cfg.hourly_budget)
                self._budget_exhausted = True
                return []
            self._budget_exhausted = False

            cur.execute(
                """WITH c AS (
                       SELECT id FROM investigations
                       WHERE (status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= now()))
                          OR (status = 'running' AND claimed_at < now() - %s::interval)
                       ORDER BY created_at, id
                       LIMIT %s
                       FOR UPDATE SKIP LOCKED
                   )
                   UPDATE investigations i
                   SET status = 'running', claimed_at = now(), attempts = i.attempts + 1, next_attempt_at = NULL
                   FROM c WHERE i.id = c.id
                   RETURNING i.id, i.user_id, i.primary_txn_id, i.txn_ids, i.incident_start, i.incident_end, i.attempts""",
                (stuck, min(n, remaining)),
            )
            cols = [d.name for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.executemany(
                "INSERT INTO investigation_attempts (investigation_id, attempt) VALUES (%s, %s)",
                [(r["id"], r["attempts"]) for r in rows],
            )
        for r in rows:
            log_event("claimed", investigation_id=r["id"], primary_txn_id=r["primary_txn_id"], attempt=r["attempts"])
        return rows

    def _has_running_rows(self) -> bool:
        conn = self._claim_conn()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM investigations WHERE status = 'running')")
            return cur.fetchone()[0]

    # ---------- one investigation ----------

    def _incident_details(self, row: dict) -> tuple[str, float, str]:
        conn = connect("worker", self.dbname)
        try:
            with conn, conn.cursor() as cur:
                # Write-once, so a retried attempt sees the same signals as the first.
                cur.execute("SELECT compute_incident_signals(%s)", (row["id"],))
                cur.execute("SELECT rule_hits FROM investigations WHERE id = %s", (row["id"],))
                rule_hits = cur.fetchone()[0] or []
                cur.execute("SELECT merchant, amount FROM transactions WHERE transaction_id = %s",
                            (row["primary_txn_id"],))
                found = cur.fetchone()
                cur.execute("SELECT count(*), coalesce(sum(amount), 0) FROM transactions "
                            "WHERE transaction_id = ANY(%s)", (row["txn_ids"],))
                n, total = cur.fetchone()
        finally:
            conn.close()
        if found is None:
            raise LookupError(f"primary transaction {row['primary_txn_id']} not found")
        merchant, amount = found
        context = (
            "This transaction is the largest in an incident flagged by the streaming anomaly rules "
            "(per-user transaction count and total spend over a 2-minute sliding window). "
            f"Incident window {row['incident_start']:%Y-%m-%d %H:%M:%S} to "
            f"{row['incident_end']:%Y-%m-%d %H:%M:%S} UTC: "
            f"{n} transaction{'' if n == 1 else 's'} totaling ${float(total):.2f}."
        )
        if self.prompt_version == "v4":
            context += f" Rules that fired: {', '.join(rule_hits) if rule_hits else 'none recorded'}."
        return merchant, float(amount), context

    def _run_baseline(self, row: dict, merchant: str, amount: float) -> dict | None:
        if self.baseline is None:
            return None
        return (self.baseline(row["user_id"], row["primary_txn_id"], merchant, amount, verbose=False)
                .get("verdict"))

    def _run_with_timeout(self, row: dict, merchant: str, amount: float, context: str) -> dict:
        box = {}

        def target():
            try:
                box["result"] = self.investigate(row["user_id"], row["primary_txn_id"], merchant, amount,
                                                 context=context, verbose=False)
            except BaseException as e:  # handed back to the worker thread below
                box["error"] = e

        # A daemon thread can't be killed; on timeout it is abandoned and its result ignored.
        t = threading.Thread(target=target, daemon=True, name=f"investigation-{row['id']}")
        t.start()
        t.join(self.cfg.timeout_seconds)
        if t.is_alive():
            raise InvestigationTimeout(f"no verdict after {self.cfg.timeout_seconds:g}s")
        if "error" in box:
            raise box["error"]
        return box["result"]

    def process(self, row: dict) -> None:
        ids = {"investigation_id": row["id"], "primary_txn_id": row["primary_txn_id"], "attempt": row["attempts"]}
        started = time.monotonic()
        baseline = None
        try:
            merchant, amount, context = self._incident_details(row)
            baseline = self._run_baseline(row, merchant, amount)
            result = self._run_with_timeout(row, merchant, amount, context)
            usage = result.get("usage")
            verdict = result.get("verdict") or {}
            if not valid_verdict(verdict, self.prompt_version) and not verdict.get("error"):
                log_event("unparseable_retry", **ids)
                result = self._run_with_timeout(row, merchant, amount, context)
                usage = sum_usage(usage, result.get("usage"))
                verdict = result.get("verdict") or {}

            trace = result.get("trace")
            if valid_verdict(verdict, self.prompt_version):
                verdict, entry = verdict_from_score(verdict)
                self._finish(row, "done", "done", verdict=verdict, trace=(trace or []) + [entry], usage=usage,
                             baseline=baseline)
            elif verdict.get("error"):
                self._finish(row, "failed", "failed", error=str(verdict["error"]), trace=trace, usage=usage,
                             baseline=baseline)
            else:
                raw = verdict.get("raw_output", verdict)
                self._finish(row, "failed", "failed", error="unparseable output after one retry",
                             trace=(trace or []) + [{"type": "unparseable_output", "content": raw}], usage=usage,
                             baseline=baseline)
        except Exception as e:
            message = f"{type(e).__name__}: {e}"[:MAX_ERROR_CHARS]
            if is_transient(e):
                outcome = "timeout" if isinstance(e, InvestigationTimeout) else "transient_error"
                self._retry_or_dead(row, outcome, message)
            else:
                self._finish(row, "failed", "failed", error=message, baseline=baseline)
        finally:
            log_event("attempt_finished", seconds=round(time.monotonic() - started, 2), **ids)

    # ---------- writes (fenced on attempt number) ----------

    def _finish(self, row: dict, status: str, outcome: str, verdict: dict | None = None,
                trace=None, error: str | None = None, usage: dict | None = None,
                baseline: dict | None = None) -> None:
        v = verdict or {}
        b = baseline or {}
        conn = connect("worker", self.dbname)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """UPDATE investigations
                       SET status = %s, finished_at = now(), provider = %s, model = %s, prompt_version = %s,
                           risk_score = %s, verdict = %s, recommended_action = %s, explanation = %s,
                           evidence = %s, trace = %s, error = %s, usage = %s,
                           baseline_score = %s, baseline_verdict = %s, baseline_action = %s
                       WHERE id = %s AND status = 'running' AND attempts = %s""",
                    (status, self.provider, self.model, self.prompt_version,
                     v.get("risk_score"), str(v["verdict"]).strip().lower() if v else None,
                     v.get("recommended_action"), v.get("explanation"),
                     Json(v.get("evidence_cited")) if v else None,
                     Json(trace) if trace is not None else None, error,
                     Json(usage) if usage else None,
                     b.get("risk_score"), b.get("verdict"), b.get("recommended_action"),
                     row["id"], row["attempts"]),
                )
                if cur.rowcount == 0:
                    log_event("stale_result_discarded", investigation_id=row["id"],
                              primary_txn_id=row["primary_txn_id"], attempt=row["attempts"])
                    return
                cur.execute(
                    """UPDATE investigation_attempts SET finished_at = now(), outcome = %s, error = %s
                       WHERE investigation_id = %s AND attempt = %s""",
                    (outcome, error, row["id"], row["attempts"]),
                )
        finally:
            conn.close()
        log_event(status, investigation_id=row["id"], primary_txn_id=row["primary_txn_id"],
                  attempt=row["attempts"], verdict=v.get("verdict"), error=error)

    def _retry_or_dead(self, row: dict, outcome: str, error: str) -> None:
        dead = row["attempts"] >= self.cfg.max_attempts
        delay = self.cfg.backoff_seconds * 2 ** (row["attempts"] - 1)
        conn = connect("worker", self.dbname)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """UPDATE investigations
                       SET status = %s, error = %s,
                           next_attempt_at = CASE WHEN %s THEN NULL ELSE now() + %s * interval '1 second' END,
                           finished_at = CASE WHEN %s THEN now() END,
                           provider = %s, model = %s, prompt_version = %s
                       WHERE id = %s AND status = 'running' AND attempts = %s""",
                    ("dead" if dead else "pending", error, dead, delay, dead,
                     self.provider, self.model, self.prompt_version, row["id"], row["attempts"]),
                )
                if cur.rowcount == 0:
                    log_event("stale_result_discarded", investigation_id=row["id"],
                              primary_txn_id=row["primary_txn_id"], attempt=row["attempts"])
                    return
                cur.execute(
                    """UPDATE investigation_attempts SET finished_at = now(), outcome = %s, error = %s
                       WHERE investigation_id = %s AND attempt = %s""",
                    (outcome, error, row["id"], row["attempts"]),
                )
        finally:
            conn.close()
        log_event("dead" if dead else "retry_scheduled", investigation_id=row["id"],
                  primary_txn_id=row["primary_txn_id"], attempt=row["attempts"], outcome=outcome,
                  retry_in_seconds=None if dead else delay, error=error)

    def _process_logged(self, row: dict) -> None:
        try:
            self.process(row)
        except Exception as e:  # e.g. database down while writing; the row is reclaimed later
            log_event("worker_error", investigation_id=row["id"], primary_txn_id=row["primary_txn_id"],
                      error=f"{type(e).__name__}: {e}"[:MAX_ERROR_CHARS])

    # ---------- main loop ----------

    def run_forever(self) -> None:
        self._loop(until_idle=False)

    def run_until_idle(self) -> None:
        """Process until nothing is claimable now and no row is still running
        (rows waiting on backoff are left for later)."""
        self._loop(until_idle=True)

    def _loop(self, until_idle: bool) -> None:
        inflight = set()
        was_enabled = None
        with ThreadPoolExecutor(max_workers=self.cfg.concurrency, thread_name_prefix="worker") as pool:
            while True:
                claimed = []
                enabled = is_enabled()
                if enabled != was_enabled:
                    log_event("enabled" if enabled else "disabled")
                    was_enabled = enabled
                if enabled:
                    try:
                        claimed = self.claim(self.cfg.concurrency - len(inflight))
                    except psycopg2.OperationalError as e:
                        log_event("database_unavailable", error=str(e)[:MAX_ERROR_CHARS])
                        if self._conn is not None:
                            self._conn.close()
                        self._conn = None
                    for row in claimed:
                        inflight.add(pool.submit(self._process_logged, row))

                if until_idle and not inflight and not claimed:
                    if not enabled or self._budget_exhausted or not self._has_running_rows():
                        break
                if inflight:
                    done, _ = wait(inflight, timeout=self.cfg.poll_seconds, return_when=FIRST_COMPLETED)
                    inflight -= done
                elif not claimed:
                    time.sleep(self.cfg.poll_seconds)
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def main():
    parser = argparse.ArgumentParser(description="Run the investigation agent on queued incidents.")
    parser.add_argument("--dbname", default=None, help="Queue database (default: PGDATABASE from .env).")
    parser.add_argument("--until-idle", action="store_true", help="Exit once the queue is drained.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.dbname:
        os.environ.setdefault("AGENT_PGDATABASE", args.dbname)  # the agent reads the same database
    worker = Worker(WorkerConfig.from_env(), dbname=args.dbname)
    log_event("started", provider=worker.provider, model=worker.model, prompt_version=worker.prompt_version,
              enabled=is_enabled(), **asdict(worker.cfg))
    try:
        worker.run_until_idle() if args.until_idle else worker.run_forever()
    except KeyboardInterrupt:
        log_event("stopped")


if __name__ == "__main__":
    main()
