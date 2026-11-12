"""
pages/review_queue.py

Cases waiting for an analyst: needs_review, held_pending_review and open QA samples,
highest review_priority first. Reads as the analyst role through review_data.
"""
import streamlit as st

import review_data

st.title("Review queue")
st.caption("Held and needs-review cases, then auto-cleared QA samples, by review priority. "
           "A hold is a recorded state only: nothing has been done to the account.")

conn = review_data.connect()
try:
    queue = review_data.queue_cases(conn)
finally:
    conn.close()

if queue.empty:
    st.info("The queue is empty.")
    st.stop()

held = int((queue["review_status"] == "held_pending_review").sum())
qa = int(queue["qa_sample"].sum())
c1, c2, c3 = st.columns(3)
c1.metric("Open cases", len(queue))
c2.metric("Held pending review", held)
c3.metric("QA samples", qa)

table = queue.assign(
    age=queue["age"].map(review_data.format_age),
    review_reasons=queue["review_reasons"].map(lambda r: ", ".join(r or [])),
    queue=queue.apply(lambda r: "QA sample" if r["qa_sample"] else r["review_status"], axis=1),
)[["id", "user_id", "amount", "verdict", "risk_score", "baseline_verdict", "baseline_score",
   "review_reasons", "age", "queue", "review_priority", "last_decision"]]

event = st.dataframe(
    table,
    hide_index=True,
    width="stretch",
    on_select="rerun",
    selection_mode="single-row",
    key="queue_table",
    column_config={
        "id": st.column_config.NumberColumn("Case", format="%d"),
        "user_id": "User",
        "amount": st.column_config.NumberColumn("Amount (USD)", format="$%.2f"),
        "verdict": "Agent verdict",
        "risk_score": st.column_config.NumberColumn("Agent score", format="%.0f"),
        "baseline_verdict": "Baseline verdict",
        "baseline_score": st.column_config.NumberColumn("Baseline score", format="%.0f"),
        "review_reasons": "Reasons",
        "age": "Age",
        "queue": "Status",
        "review_priority": st.column_config.NumberColumn("Priority", format="%d"),
        "last_decision": "Last decision",
    },
)

rows = event.selection.rows if event else []
if rows:
    case_id = int(table.iloc[rows[0]]["id"])
    if st.button(f"Open case {case_id}", type="primary"):
        st.session_state["case_id"] = case_id
        st.switch_page("pages/case_detail.py")
else:
    st.caption("Select a row to open the case.")
