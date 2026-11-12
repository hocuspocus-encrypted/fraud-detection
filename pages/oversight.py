"""
pages/oversight.py

Review oversight: cases by review_status, queue depth, median time from routing to the
closing decision, and the override rate. Reads as the analyst role through review_data.
"""
import pandas as pd
import streamlit as st

import review_data

st.title("Oversight")

conn = review_data.connect()
try:
    o = review_data.oversight(conn)
finally:
    conn.close()

median = o["median_time_to_decision"]
rate = o["override_rate"]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Queue depth", o["queue_depth"], help="needs_review + held_pending_review + open QA samples")
c2.metric("Cases decided", o["decided_cases"], help="Closed by agree or override")
c3.metric("Median time to decision", review_data.format_age(median) or "n/a",
          help="From routing (investigation finished) to the first agree or override")
c4.metric("Override rate", "n/a" if rate is None else f"{rate:.0%}",
          help="Share of decided cases whose latest closing decision is an override")

st.markdown("**Cases by review status**")
counts = pd.DataFrame({"review_status": list(o["by_status"]), "cases": list(o["by_status"].values())})
st.dataframe(counts.sort_values("cases", ascending=False), hide_index=True)

st.info("Agreement rate, override direction, QA-sample override rate and the share routed to review "
        "come with Phase H6 (v_oversight_metrics).")
