"""
streamlit_dashboard.py

Entry point of the multipage dashboard. "Pipeline" is the original live dashboard; the
review pages (docs/PLAN_HITL.md Phase H5) connect as the analyst role through
review_data.py, never as fraud_user.

Run with (from the repo root):
    venv\\Scripts\\python.exe -m streamlit run streamlit_dashboard.py
"""
import streamlit as st

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")

page = st.navigation({
    "Pipeline": [
        st.Page("pages/pipeline.py", title="Pipeline", icon=":material/monitoring:", default=True),
    ],
    "Review": [
        st.Page("pages/review_queue.py", title="Review queue", icon=":material/inbox:"),
        st.Page("pages/case_detail.py", title="Case detail", icon=":material/fact_check:"),
        st.Page("pages/oversight.py", title="Oversight", icon=":material/insights:"),
    ],
})
page.run()
