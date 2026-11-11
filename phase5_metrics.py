"""
phase5_metrics.py

Measures a live run of the whole pipeline (PLAN.md Phase 5) from what's in
Postgres, for docs/METRICS.md. Read-only; connects as admin.

Latency is measured from the incident's FIRST TRANSACTION, because
anomaly_flags.detected_at is reset on every upsert and so doesn't keep the
moment a window was first flagged. That makes "total" an upper bound on
flag-to-verdict. Stages (seconds):
    settle         first transaction -> incident queued (window + watermark + enqueuer settle delay)
    queue_wait     queued -> first claim by the worker
    investigation  last claim -> verdict
    total          first transaction -> verdict
Only incidents that reached 'done' count toward latency.

Spark health per bucket: transactions ingested, ingest lag (ingested_at -
event_timestamp, raw-ingest job) and anomaly_flags rows written. Comparing
buckets with the worker on and off shows whether the worker affects Spark.

Usage:
    venv\\Scripts\\python.exe phase5_metrics.py --since 2026-10-22T10:00:00Z [--until ...]
        [--period "worker on=10:00/10:30" --period "worker off=10:30/10:50" ...]
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone

from db import connect

CUST = "cust\\_%"  # generated customers only (LIKE pattern)


def percentile(values: list[float], q: float) -> float | None:
    """Linear interpolation between closest ranks (numpy's default)."""
    if not values:
        return None
    v = sorted(values)
    pos = (len(v) - 1) * q
    lo, hi = int(pos), min(int(pos) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def _summary(values: list[float]) -> dict:
    return {"p50": percentile(values, 0.5), "p95": percentile(values, 0.95),
            "max": max(values) if values else None}


def latency_stats(conn, since: datetime, until: datetime) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT extract(epoch FROM i.finished_at - t.first_txn),
                      extract(epoch FROM i.created_at - t.first_txn),
                      extract(epoch FROM a.first_claim - i.created_at),
                      extract(epoch FROM i.finished_at - i.claimed_at)
               FROM investigations i
               CROSS JOIN LATERAL (SELECT min(event_timestamp) AS first_txn FROM transactions
                                   WHERE transaction_id = ANY(i.txn_ids)) t
               CROSS JOIN LATERAL (SELECT min(started_at) AS first_claim FROM investigation_attempts
                                   WHERE investigation_id = i.id) a
               WHERE i.status = 'done' AND i.user_id LIKE %s AND i.created_at >= %s AND i.created_at < %s""",
            (CUST, since, until),
        )
        rows = cur.fetchall()
    cols = list(zip(*rows)) if rows else [[], [], [], []]
    total, settle, queue_wait, investigation = ([float(x) for x in c] for c in cols)
    return {"n": len(rows), "total": _summary(total), "settle": _summary(settle),
            "queue_wait": _summary(queue_wait), "investigation": _summary(investigation)}


def outstanding_series(conn, since: datetime, until: datetime, step_minutes: int = 5) -> list[tuple]:
    """(time, incidents queued and not yet finished) at each step."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT ts, (SELECT count(*) FROM investigations i
                           WHERE i.user_id LIKE %s AND i.created_at <= ts
                             AND (i.finished_at IS NULL OR i.finished_at > ts))
               FROM generate_series(%s::timestamptz, %s::timestamptz, make_interval(mins => %s)) ts
               ORDER BY ts""",
            (CUST, since, until, step_minutes),
        )
        return cur.fetchall()


def failure_summary(conn, since: datetime, until: datetime) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT status, count(*) FROM investigations WHERE user_id LIKE %s "
                    "AND created_at >= %s AND created_at < %s GROUP BY 1", (CUST, since, until))
        by_status = dict(cur.fetchall())
        cur.execute("""SELECT a.outcome, count(*) FROM investigation_attempts a JOIN investigations i
                         ON i.id = a.investigation_id
                       WHERE i.user_id LIKE %s AND i.created_at >= %s AND i.created_at < %s
                         AND a.outcome IS NOT NULL GROUP BY 1""", (CUST, since, until))
        outcomes = dict(cur.fetchall())
    finished = sum(n for s, n in by_status.items() if s in ("done", "failed", "dead"))
    bad = by_status.get("failed", 0) + by_status.get("dead", 0)
    return {"by_status": by_status, "attempt_outcomes": outcomes,
            "failure_rate": bad / finished if finished else None}


def spark_health(conn, since: datetime, until: datetime, bucket_minutes: int = 5) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """WITH b AS (SELECT ts AS bucket, ts + make_interval(mins => %(m)s) AS bucket_end
                          FROM generate_series(%(since)s::timestamptz, %(until)s::timestamptz - interval '1 second',
                                               make_interval(mins => %(m)s)) ts)
               SELECT b.bucket,
                      (SELECT count(*) FROM transactions t WHERE t.user_id LIKE %(cust)s
                         AND t.ingested_at >= b.bucket AND t.ingested_at < b.bucket_end),
                      (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM t.ingested_at - t.event_timestamp))
                         FROM transactions t WHERE t.user_id LIKE %(cust)s
                         AND t.ingested_at >= b.bucket AND t.ingested_at < b.bucket_end),
                      (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM t.ingested_at - t.event_timestamp))
                         FROM transactions t WHERE t.user_id LIKE %(cust)s
                         AND t.ingested_at >= b.bucket AND t.ingested_at < b.bucket_end),
                      (SELECT count(*) FROM anomaly_flags f WHERE f.user_id LIKE %(cust)s
                         AND f.detected_at >= b.bucket AND f.detected_at < b.bucket_end)
               FROM b ORDER BY b.bucket""",
            {"m": bucket_minutes, "since": since, "until": until, "cust": CUST},
        )
        return [{"bucket": r[0], "txns": r[1], "ingest_lag_p50": r[2], "ingest_lag_p95": r[3], "flag_rows": r[4]}
                for r in cur.fetchall()]


def stream_gaps(conn, since: datetime, until: datetime, threshold_seconds: float = 60) -> list[tuple]:
    """(gap start, gap end, seconds) wherever no generated transaction was ingested for longer
    than threshold_seconds. At 2 txn/s a gap means the producer, Spark or the whole machine
    stopped (a 2026-10-22 run was ruined by the laptop sleeping), so timings are invalid."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT prev, ingested_at, extract(epoch FROM ingested_at - prev)
               FROM (SELECT ingested_at, lag(ingested_at) OVER (ORDER BY ingested_at) AS prev
                     FROM transactions
                     WHERE user_id LIKE %s AND ingested_at >= %s AND ingested_at < %s) x
               WHERE prev IS NOT NULL AND ingested_at - prev > make_interval(secs => %s)
               ORDER BY prev""",
            (CUST, since, until, threshold_seconds),
        )
        return [(a, b, float(s)) for a, b, s in cur.fetchall()]


def agent_vs_ground_truth(conn, since: datetime, until: datetime) -> Counter:
    """(does the incident contain a labeled fraud transaction?, agent verdict) -> count, for done incidents."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT CASE WHEN EXISTS (SELECT 1 FROM transactions t WHERE t.transaction_id = ANY(i.txn_ids)
                                        AND t.is_fraud) THEN 'fraud' ELSE 'no_fraud' END, i.verdict
               FROM investigations i
               WHERE i.status = 'done' AND i.user_id LIKE %s AND i.created_at >= %s AND i.created_at < %s""",
            (CUST, since, until),
        )
        return Counter((truth, verdict) for truth, verdict in cur.fetchall())


# ---------- report ----------

def _fmt(seconds) -> str:
    if seconds is None:
        return "-"
    return f"{seconds / 60:.1f} min" if seconds >= 120 else f"{seconds:.0f} s"


def _parse_time(value: str, day: datetime) -> datetime:
    if "T" in value:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    h, m = (int(x) for x in value.split(":")[:2])
    return day.replace(hour=h, minute=m, second=0, microsecond=0)


def report(since: datetime, until: datetime, periods: list[tuple[str, datetime, datetime]]) -> str:
    conn = connect("admin")
    try:
        lines = [f"Window: {since:%Y-%m-%d %H:%M} to {until:%H:%M} UTC", ""]
        gaps = stream_gaps(conn, since, until)
        if gaps:
            lines += ["**WARNING: the transaction stream stopped during this window, so the timings below are "
                      "invalid** (machine asleep, or producer/Spark stopped):", ""]
            lines += [f"- {a:%Y-%m-%d %H:%M:%S} to {b:%Y-%m-%d %H:%M:%S} UTC ({s / 60:.1f} min)" for a, b, s in gaps]
            lines.append("")
        lat = latency_stats(conn, since, until)
        lines += [f"**Latency** ({lat['n']} incidents done)", "",
                  "| Stage | p50 | p95 | max |", "|---|---|---|---|"]
        for stage in ("settle", "queue_wait", "investigation", "total"):
            s = lat[stage]
            lines.append(f"| {stage} | {_fmt(s['p50'])} | {_fmt(s['p95'])} | {_fmt(s['max'])} |")

        f = failure_summary(conn, since, until)
        rate = "-" if f["failure_rate"] is None else f"{f['failure_rate']:.1%}"
        lines += ["", f"**Outcomes:** {f['by_status']}; attempts {f['attempt_outcomes']}; failure rate {rate}"]

        lines += ["", "**Queue depth** (queued, not finished)", "", "| Time (UTC) | Outstanding |", "|---|---|"]
        lines += [f"| {t:%H:%M} | {n} |" for t, n in outstanding_series(conn, since, until)]

        lines += ["", "**Spark health** (5-min buckets)", "",
                  "| Bucket (UTC) | Period | Txns ingested | Ingest lag p50 | p95 | Flag rows |", "|---|---|---|---|---|---|"]
        for b in spark_health(conn, since, until):
            period = next((name for name, s, e in periods if s <= b["bucket"] < e), "")
            p50 = "-" if b["ingest_lag_p50"] is None else f"{b['ingest_lag_p50']:.1f} s"
            p95 = "-" if b["ingest_lag_p95"] is None else f"{b['ingest_lag_p95']:.1f} s"
            lines.append(f"| {b['bucket']:%H:%M} | {period} | {b['txns']} | {p50} | {p95} | {b['flag_rows']} |")

        if periods:
            lines += ["", "| Period | Txns/min | Ingest lag p50 (median of buckets) | Flag rows/min |", "|---|---|---|---|"]
            buckets = spark_health(conn, since, until)
            for name, s, e in periods:
                inside = [b for b in buckets if s <= b["bucket"] < e and b["txns"]]
                if not inside:
                    continue
                minutes = len(inside) * 5
                lags = [b["ingest_lag_p50"] for b in inside if b["ingest_lag_p50"] is not None]
                lines.append(f"| {name} | {sum(b['txns'] for b in inside) / minutes:.1f} | "
                             f"{percentile(lags, 0.5):.1f} s | {sum(b['flag_rows'] for b in inside) / minutes:.1f} |")

        m = agent_vs_ground_truth(conn, since, until)
        lines += ["", "**Agent verdict vs ground truth** (done incidents; informational, Phase 9 evaluates properly)", "",
                  "| Incident contains labeled fraud | legitimate | suspicious | likely_fraud |", "|---|---|---|---|"]
        for truth in ("fraud", "no_fraud"):
            lines.append(f"| {'yes' if truth == 'fraud' else 'no'} | " +
                         " | ".join(str(m.get((truth, v), 0)) for v in ("legitimate", "suspicious", "likely_fraud")) + " |")
        return "\n".join(lines)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Phase 5 metrics from Postgres.")
    parser.add_argument("--since", required=True, help="ISO time, e.g. 2026-10-22T10:00:00Z")
    parser.add_argument("--until", default=None, help="ISO time (default: now)")
    parser.add_argument("--period", action="append", default=[],
                        help='"name=HH:MM/HH:MM" (UTC, same day as --since); repeatable')
    args = parser.parse_args()
    since = _parse_time(args.since, datetime.now(timezone.utc))
    until = _parse_time(args.until, since) if args.until else datetime.now(timezone.utc)
    periods = []
    for p in args.period:
        name, span = p.split("=", 1)
        start, end = span.split("/")
        periods.append((name, _parse_time(start, since), _parse_time(end, since)))
    print(report(since, until, periods))


if __name__ == "__main__":
    main()
