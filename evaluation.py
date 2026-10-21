"""
evaluation.py

Scores the rolling-window anomaly detector (spark_fraud_detection.py)
against the synthetic ground-truth `is_fraud` labels from the generator.

Evaluation is per transaction:
    predicted fraud = the transaction falls inside a flagged window
                      (anomaly_flags row for the same user covering its timestamp)
    actual fraud    = transactions.is_fraud

Transactions newer than SETTLE_MINUTES are excluded: their windows may not
have closed yet (window + watermark + trigger lag), so they'd be counted as
false negatives purely because the detector hasn't caught up.

Used by streamlit_dashboard.py, or run standalone:
    venv\\Scripts\\python.exe evaluation.py --minutes 60
    venv\\Scripts\\python.exe evaluation.py            (all time)
"""

import argparse

from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://fraud_user:fraud_pass@localhost:5432/fraud_db"

# 2-min window + 2-min watermark + ~30s trigger/slide lag, rounded up.
SETTLE_MINUTES = 5

CONFUSION_SQL = text(
    """
    WITH scored AS (
        SELECT
            t.is_fraud,
            EXISTS (
                SELECT 1 FROM anomaly_flags a
                WHERE a.user_id = t.user_id
                  AND t.event_timestamp >= a.window_start
                  AND t.event_timestamp <  a.window_end
            ) AS flagged
        FROM transactions t
        WHERE t.event_timestamp < now() - make_interval(mins => :settle)
          AND (CAST(:minutes AS INTEGER) IS NULL
               OR t.event_timestamp >= now() - make_interval(mins => CAST(:minutes AS INTEGER)))
    )
    SELECT
        COUNT(*) FILTER (WHERE flagged AND is_fraud)         AS tp,
        COUNT(*) FILTER (WHERE flagged AND NOT is_fraud)     AS fp,
        COUNT(*) FILTER (WHERE NOT flagged AND is_fraud)     AS fn,
        COUNT(*) FILTER (WHERE NOT flagged AND NOT is_fraud) AS tn
    FROM scored
    """
)


def _safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


def evaluate(engine, minutes: int | None = None) -> dict:
    """Return confusion-matrix counts and classification metrics.

    minutes: only score transactions from the last N minutes (None = all time).
    """
    with engine.connect() as conn:
        row = conn.execute(CONFUSION_SQL, {"settle": SETTLE_MINUTES, "minutes": minutes}).one()

    tp, fp, fn, tn = (int(v) for v in row)
    total = tp + fp + fn + tn
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "total": total,
        "precision": precision,
        "recall": recall,
        "f1": _safe_div(2 * precision * recall, precision + recall),
        "accuracy": _safe_div(tp + tn, total),
        # Fraction of transactions that are actually fraud. A detector that
        # flags at random gets precision ~= this, so it's the baseline to beat.
        "fraud_rate": _safe_div(tp + fn, total),
        "flag_rate": _safe_div(tp + fp, total),
    }


def main():
    parser = argparse.ArgumentParser(description="Evaluate the anomaly detector against is_fraud labels.")
    parser.add_argument("--minutes", type=int, default=None, help="Only score the last N minutes (default: all time).")
    args = parser.parse_args()

    m = evaluate(create_engine(DB_URL), args.minutes)
    scope = f"last {args.minutes} min" if args.minutes else "all time"
    print(f"Scored {m['total']:,} transactions ({scope}, excluding the most recent {SETTLE_MINUTES} min)\n")
    print("                 actual fraud   actual legit")
    print(f"  flagged        {m['tp']:>12,}   {m['fp']:>12,}")
    print(f"  not flagged    {m['fn']:>12,}   {m['tn']:>12,}\n")
    print(f"  precision  {m['precision']:.3f}   (baseline = fraud rate {m['fraud_rate']:.3f})")
    print(f"  recall     {m['recall']:.3f}")
    print(f"  f1         {m['f1']:.3f}")
    print(f"  accuracy   {m['accuracy']:.3f}")


if __name__ == "__main__":
    main()
