import streamlit as st
import pandas as pd
import altair as alt
from sqlalchemy import create_engine

# ---------------------------------------------------
# Page Config
# ---------------------------------------------------
st.set_page_config(
    page_title="SaaS Executive Dashboard",
    layout="wide"
)

# ---------------------------------------------------
# Database Connection
# ---------------------------------------------------
engine = create_engine(
    "postgresql+psycopg2://saas_user:saas_pass@localhost:5432/saas_analytics"
)

query = """
SELECT *
FROM gold.dashboard_monthly
ORDER BY month
"""

df = pd.read_sql(query, engine)

# ---------------------------------------------------
# Data Preparation
# ---------------------------------------------------
df["month"] = pd.to_datetime(df["month"])
df["month_str"] = df["month"].dt.strftime("%Y-%m")

latest = df.iloc[-1]
avg_churn_3m = df["churn_rate"].tail(3).mean()

# ---------------------------------------------------
# Header + KPIs
# ---------------------------------------------------
st.title("📊 SaaS Executive Dashboard")
st.caption("Gold-layer metrics • Monthly granularity")

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "MRR",
    f"₹{latest['mrr'] / 1_000_000:.2f}M"
)

c2.metric(
    "Active Customers",
    f"{int(latest['active_customers']):,}"
)

c3.metric(
    "Churn Rate (3M Avg)",
    f"{avg_churn_3m:.2%}"
)

c4.metric(
    "DAU / MAU",
    f"{latest['dau_mau_ratio']:.2%}"
)

st.markdown("---")

# ---------------------------------------------------
# Revenue & Customers (BAR CHARTS)
# ---------------------------------------------------
st.subheader("📈 Revenue & Customer Growth")

col1, col2 = st.columns(2)

with col1:
    mrr_chart = (
        alt.Chart(df)
        .mark_bar(color="#4C78A8")
        .encode(
            x=alt.X("month_str:N", title="Month", sort=None),
            y=alt.Y("mrr:Q", title="MRR"),
            tooltip=["month_str", alt.Tooltip("mrr:Q", format=",.0f")]
        )
        .properties(height=320)
    )
    st.altair_chart(mrr_chart, use_container_width=True)

with col2:
    active_chart = (
        alt.Chart(df)
        .mark_bar(color="#72B7B2")
        .encode(
            x=alt.X("month_str:N", title="Month", sort=None),
            y=alt.Y("active_customers:Q", title="Active Customers"),
            tooltip=["month_str", "active_customers"]
        )
        .properties(height=320)
    )
    st.altair_chart(active_chart, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------
# Retention & Engagement (LINE CHARTS)
# ---------------------------------------------------
st.subheader("📉 Retention & Engagement")

col3, col4 = st.columns(2)

with col3:
    churn_chart = (
        alt.Chart(df)
        .mark_line(point=True, color="#E45756")
        .encode(
            x=alt.X("month_str:N", title="Month", sort=None),
            y=alt.Y(
                "churn_rate:Q",
                title="Churn Rate",
                axis=alt.Axis(format="%")
            ),
            tooltip=[
                "month_str",
                alt.Tooltip("churn_rate:Q", format=".2%")
            ]
        )
        .properties(height=320)
    )
    st.altair_chart(churn_chart, use_container_width=True)

with col4:
    engagement_chart = (
        alt.Chart(df)
        .mark_line(point=True, color="#F58518")
        .encode(
            x=alt.X("month_str:N", title="Month", sort=None),
            y=alt.Y(
                "dau_mau_ratio:Q",
                title="DAU / MAU",
                axis=alt.Axis(format="%")
            ),
            tooltip=[
                "month_str",
                alt.Tooltip("dau_mau_ratio:Q", format=".2%")
            ]
        )
        .properties(height=320)
    )
    st.altair_chart(engagement_chart, use_container_width=True)

# ---------------------------------------------------
# Footer
# ---------------------------------------------------
st.markdown("---")
st.caption(
    "Built on Postgres Gold layer • Streamlit for visualization • Metrics modeled in warehouse"
)
