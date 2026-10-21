"""
investigation_enqueuer.py

Polls Postgres and turns settled anomaly_flags into investigation incidents
(PLAN.md D2). All the merge/settle/dedupe logic lives in the SQL function
enqueue_incidents() (db/migrations/0004_investigations.sql); this loop just
calls it on a timer as the `worker` role. It never touches Spark or Kafka.

Usage:
    venv\\Scripts\\python.exe investigation_enqueuer.py          (loop forever)
    venv\\Scripts\\python.exe investigation_enqueuer.py --once   (one pass, e.g. for a backfill)

Settings come from .env: ENQUEUE_SETTLE, ENQUEUE_GRACE, ENQUEUE_SINCE, ENQUEUE_POLL_SECONDS.
"""
import argparse
import logging
import os
import time

import psycopg2

from db import connect

log = logging.getLogger("enqueuer")


def enqueue_once(conn, settle: str, grace: str, since: str) -> tuple[int, int]:
    """Returns (rows enqueued this pass, pending rows in the queue)."""
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT enqueue_incidents(%s::interval, %s::interval, %s::timestamptz)",
            (settle, grace, since),
        )
        enqueued = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM investigations WHERE status = 'pending'")
        pending = cur.fetchone()[0]
    return enqueued, pending


def main():
    parser = argparse.ArgumentParser(description="Enqueue settled anomaly incidents for investigation.")
    parser.add_argument("--once", action="store_true", help="Run a single pass and exit.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settle = os.getenv("ENQUEUE_SETTLE", "5 minutes")
    grace = os.getenv("ENQUEUE_GRACE", "30 minutes")
    since = os.getenv("ENQUEUE_SINCE", "-infinity")
    poll_seconds = float(os.getenv("ENQUEUE_POLL_SECONDS", "30"))
    log.info("settle=%s grace=%s since=%s poll=%ss", settle, grace, since, poll_seconds)

    conn = None
    try:
        while True:
            try:
                if conn is None or conn.closed:
                    conn = connect("worker")
                enqueued, pending = enqueue_once(conn, settle, grace, since)
                if enqueued or args.once:
                    log.info("enqueued=%d pending=%d", enqueued, pending)
            except psycopg2.OperationalError as e:
                # Postgres restarting or unreachable: drop the connection and retry next tick.
                log.warning("database unavailable, retrying in %ss: %s", poll_seconds, e)
                if conn is not None:
                    conn.close()
                conn = None
            if args.once:
                break
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
