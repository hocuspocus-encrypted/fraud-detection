"""
pages/case_detail.py

One case: incident transactions, rule hits, signals, the agent's explanation and evidence
(with the tool each claim cites), the tool trace, the baseline result and earlier decisions,
then the decision form. Submits through record_decision() as the analyst role.

Text from the database (explanation, evidence, merchant names, reasons) is data: it is
shown with st.text or in tables, never rendered as Markdown.
"""
import getpass
from datetime import timezone

import pandas as pd
import streamlit as st

import review_data

MAX_TEXT = 4000   # display cap for free text from the database

st.title("Case detail")

conn = review_data.connect()
try:
    queue_ids = [int(i) for i in review_data.queue_cases(conn)["id"]]
    current = st.session_state.get("case_id")
    options = queue_ids if current is None or current in queue_ids else [current] + queue_ids
    if not options:
        st.info("The queue is empty and no case is selected. Open one from the review queue.")
        st.stop()
    case_id = st.selectbox("Case", options, index=options.index(current) if current in options else 0,
                           help="Open cases, highest priority first.")
    st.session_state["case_id"] = case_id
    detail = review_data.case_detail(conn, case_id)
finally:
    conn.close()

if detail is None:
    st.error(f"Case {case_id} does not exist.")
    st.stop()

flash = st.session_state.pop("flash", None)
if flash:
    st.success(flash)

case = detail["case"]
open_now = case_id in queue_ids


def plain(value) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= MAX_TEXT else text[:MAX_TEXT] + " [truncated]"


def score(value) -> str:
    return "n/a" if value is None else f"{value:.0f}"


# ---------- summary ----------

status = "QA sample (auto-cleared)" if case["qa_sample"] and case["review_status"] == "auto_cleared" \
    else (case["review_status"] or "not routed")
st.subheader(f"Case {case_id} · {case['user_id']} · {status}")
if not open_now:
    st.caption("This case is not in the queue. A new decision is still recorded if the case allows one.")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Agent verdict", case["verdict"] or "n/a", help=f"Recommended: {case['recommended_action']}")
c2.metric("Agent score", score(case["risk_score"]))
c3.metric("Baseline verdict", case["baseline_verdict"] or "n/a",
          help=f"Rule scorer, not an ML model. Recommended: {case['baseline_action']}")
c4.metric("Baseline score", score(case["baseline_score"]))

st.markdown("**Why it is in review:** " + (", ".join(f"`{r}`" for r in case["review_reasons"] or []) or "none")
            + f" · priority {case['review_priority']}")
st.markdown("**Rules that flagged it:** "
            + (", ".join(f"`{r}`" for r in case["rule_hits"] or []) or "none recorded"))
st.caption(f"Incident {case['incident_start'].astimezone(timezone.utc):%Y-%m-%d %H:%M:%S} to "
           f"{case['incident_end'].astimezone(timezone.utc):%H:%M:%S} UTC · "
           f"investigation {case['status']} after {case['attempts']} attempt(s) · "
           f"{case['provider'] or '?'} / {case['model'] or '?'} / prompt {case['prompt_version'] or '?'}")
if case["error"]:
    st.warning("Investigation error:")
    st.text(plain(case["error"]))

# ---------- incident ----------

left, right = st.columns([3, 2])
with left:
    st.markdown("**Incident transactions**")
    st.dataframe(detail["transactions"], hide_index=True, width="stretch",
                 column_config={"amount": st.column_config.NumberColumn(format="$%.2f"),
                                "primary": st.column_config.CheckboxColumn("Primary")})
with right:
    st.markdown("**Signals**")
    signals = case["signals"] or {}
    if signals:
        st.dataframe(pd.DataFrame({"signal": list(signals), "value": [str(v) for v in signals.values()]}),
                     hide_index=True, width="stretch")
    else:
        st.caption("No signals recorded.")

# ---------- agent ----------

st.markdown("**Agent explanation**")
st.text(plain(case["explanation"]) or "No explanation (the investigation did not finish).")
st.caption(f"Confidence: {case['confidence'] or 'n/a'} · grounding check: "
           f"{ {True: 'passed', False: 'failed', None: 'n/a (prompt before v4)'}[case['grounding_ok']] }")

st.markdown("**Evidence cited**")
if detail["evidence"]:
    st.dataframe(pd.DataFrame([{"claim": plain(e["claim"]), "tool cited": e["tool"] or "(none named)"}
                               for e in detail["evidence"]]),
                 hide_index=True, width="stretch")
else:
    st.caption("No evidence cited.")

with st.expander(f"Tool trace ({len(detail['trace'])} steps)"):
    for step in detail["trace"]:
        label = step.get("tool") or step.get("type", "step")
        st.markdown(f"Step {step.get('step', '?')} · `{label}`" + (" (prefetched)" if step.get("prefetched") else ""))
        st.json(step, expanded=False)

# ---------- decisions ----------

st.markdown("**Previous decisions**")
if detail["decisions"].empty:
    st.caption("None yet.")
else:
    st.dataframe(detail["decisions"], hide_index=True, width="stretch")

st.divider()
st.subheader("Decision")
st.caption("Agree or override closes the case; request info records the request and keeps it in the queue. "
           "The final action is recorded for a person to carry out: nothing is done to the account here.")

with st.form("decision_form"):
    analyst = st.text_input("Your name", value=st.session_state.get("analyst_last") or getpass.getuser(),
                            max_chars=review_data.MAX_ANALYST, key="analyst_name")
    decision = st.radio("Decision", review_data.DECISIONS, horizontal=True, key="decision",
                        format_func=lambda d: d.replace("_", " "),
                        help=f"The agent recommended: {case['recommended_action']}")
    final_action = st.selectbox("Final action", review_data.FINAL_ACTIONS, index=None,
                                placeholder="Choose one (optional for request info)", key="final_action")
    reason = st.text_area("Reason (required for an override)", max_chars=review_data.MAX_REASON, key="reason")
    submitted = st.form_submit_button("Submit decision", type="primary")

if submitted:
    errors = review_data.validate_decision(decision, final_action, reason, analyst)
    if errors:
        for e in errors:
            st.error(e)
    else:
        conn = review_data.connect()
        try:
            decision_id = review_data.submit_decision(conn, case_id, analyst, decision, final_action, reason)
        except review_data.DecisionError as e:
            st.error(f"Not recorded: {e}")
        else:
            st.session_state["analyst_last"] = analyst.strip()
            closed = decision != "request_info"
            st.session_state["flash"] = (f"Decision {decision_id} recorded for case {case_id}: "
                                         f"{decision.replace('_', ' ')}"
                                         + (f", {final_action}" if final_action else "")
                                         + (". The case left the queue." if closed else ". The case stays in the queue."))
            for key in ("decision", "final_action", "reason"):
                st.session_state.pop(key, None)
            st.rerun()
        finally:
            conn.close()
