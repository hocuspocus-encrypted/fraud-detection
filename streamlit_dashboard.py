"""
streamlit_dashboard.py

A basic live dashboard for the fraud detection pipeline. Reads directly
from PostgreSQL (populated by spark_consumer_postgres.py and
spark_fraud_detection.py) and auto-refreshes every few seconds to reflect
the live, growing dataset.

Run with:
    pip install streamlit streamlit-autorefresh sqlalchemy pandas altair
    venv\\Scripts\\python.exe -m streamlit run streamlit_dashboard.py
"""

import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

# Matches docker-compose.yml postgres service credentials
DB_URL = "postgresql+psycopg2://fraud_user:fraud_pass@localhost:5432/fraud_db"

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")

# Refresh the whole page every 3 seconds to pull in new rows.
st_autorefresh(interval=3000, key="dashboard_refresh")


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


def run_query(sql: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(sql, engine)


st.title("Real-Time Fraud Detection Dashboard")
st.caption("Auto-refreshes every 3 seconds • Data from Kafka → Spark → PostgreSQL")

# --- Top-line metrics ---------------------------------------------------
summary = run_query(
    """
    SELECT
        (SELECT COUNT(*) FROM transactions) AS total_transactions,
        (SELECT COUNT(*) FROM transactions WHERE is_fraud = true) AS labeled_fraud,
        (SELECT COUNT(*) FROM anomaly_flags) AS total_anomalies,
        (SELECT COALESCE(AVG(amount), 0) FROM transactions) AS avg_amount,
        (SELECT COALESCE(MAX(event_timestamp), now()) FROM transactions) AS latest_ts
    """
)
row = summary.iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Transactions", f"{int(row['total_transactions']):,}")
col2.metric("Labeled Fraud (synthetic)", f"{int(row['labeled_fraud']):,}")
col3.metric("Anomalies Flagged", f"{int(row['total_anomalies']):,}")
col4.metric("Avg Transaction Amount", f"${row['avg_amount']:.2f}")

st.divider()

# --- Transaction volume over time ---------------------------------------
st.subheader("Transaction Volume (last 30 minutes)")
volume_df = run_query(
    """
    SELECT
        date_trunc('minute', event_timestamp) AS minute,
        COUNT(*) AS txn_count
    FROM transactions
    WHERE event_timestamp > now() - interval '30 minutes'
    GROUP BY minute
    ORDER BY minute
    """
)

if not volume_df.empty:
    chart = (
        alt.Chart(volume_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("minute:T", title="Time"),
            y=alt.Y("txn_count:Q", title="Transactions per minute"),
        )
        .properties(height=300)
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("No transactions in the last 30 minutes yet. Is the producer running?")

st.divider()

# --- Two side-by-side panels: recent transactions + recent anomalies ---
left, right = st.columns(2)

with left:
    st.subheader("Most Recent Transactions")
    recent_txns = run_query(
        """
        SELECT event_timestamp, user_id, amount, merchant_category, card_type, is_fraud
        FROM transactions
        ORDER BY event_timestamp DESC
        LIMIT 15
        """
    )
    st.dataframe(recent_txns, use_container_width=True, hide_index=True)

with right:
    st.subheader("Flagged Anomalies")
    recent_anomalies = run_query(
        """
        SELECT detected_at, user_id, window_start, window_end, txn_count, total_amount
        FROM anomaly_flags
        ORDER BY detected_at DESC
        LIMIT 15
        """
    )
    if recent_anomalies.empty:
        st.info("No anomalies flagged yet.")
    else:
        st.dataframe(recent_anomalies, use_container_width=True, hide_index=True)

st.divider()

# --- Spend by merchant category -----------------------------------------
st.subheader("Spend by Merchant Category (all time)")
category_df = run_query(
    """
    SELECT merchant_category, COUNT(*) AS txn_count, SUM(amount) AS total_spend
    FROM transactions
    GROUP BY merchant_category
    ORDER BY total_spend DESC
    """
)

if not category_df.empty:
    bar_chart = (
        alt.Chart(category_df)
        .mark_bar()
        .encode(
            x=alt.X("merchant_category:N", sort="-y", title="Category"),
            y=alt.Y("total_spend:Q", title="Total Spend ($)"),
            tooltip=["merchant_category", "txn_count", "total_spend"],
        )
        .properties(height=300)
    )
    st.altair_chart(bar_chart, use_container_width=True)
