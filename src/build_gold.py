import pandas as pd
from sqlalchemy import text
from config.db import get_engine


def build_gold_mrr_monthly():
    engine = get_engine()

    query = """
        SELECT
            subscription_id,
            start_date,
            end_date,
            monthly_amount
        FROM silver.subscriptions
        WHERE is_active = true
    """

    df = pd.read_sql(query, engine)

    # Ensure datetime
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    rows = []

    for _, row in df.iterrows():
        start = row["start_date"].replace(day=1)
        end = (
            row["end_date"].replace(day=1)
            if pd.notnull(row["end_date"])
            else start
        )

        for month in pd.date_range(start, end, freq="MS"):
            rows.append(
                {
                    "month": month,
                    "subscription_id": row["subscription_id"],
                    "monthly_amount": row["monthly_amount"],
                }
            )

    expanded = pd.DataFrame(rows)

    result = (
        expanded
        .groupby("month")
        .agg(
            mrr=("monthly_amount", "sum"),
            active_subscriptions=("subscription_id", "nunique"),
        )
        .reset_index()
    )

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold.mrr_monthly"))

        result.to_sql(
            name="mrr_monthly",
            schema="gold",
            con=conn,
            index=False,
            if_exists="replace",
        )

    print("✅ gold.mrr_monthly built successfully")


def build_gold_customer_churn():
    engine = get_engine()

    query = """
        SELECT
            customer_id,
            start_date,
            end_date
        FROM silver.subscriptions
    """

    df = pd.read_sql(query, engine)

    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    rows = []

    for _, row in df.iterrows():
        start = row["start_date"].replace(day=1)
        end = (
            row["end_date"].replace(day=1)
            if pd.notnull(row["end_date"])
            else start
        )

        for month in pd.date_range(start, end, freq="MS"):
            rows.append(
                {
                    "customer_id": row["customer_id"],
                    "month": month,
                }
            )

    activity = pd.DataFrame(rows)

    # Active customers per month
    monthly_active = (
        activity.groupby("month")["customer_id"]
        .nunique()
        .reset_index(name="active_customers")
    )

    # Detect churn
    activity["prev_month"] = activity["month"] + pd.offsets.MonthBegin(1)

    churn = (
        activity.merge(
            activity,
            left_on=["customer_id", "prev_month"],
            right_on=["customer_id", "month"],
            how="left",
            suffixes=("", "_next"),
        )
        .query("month_next.isna()")
    )

    churned = (
        churn.groupby("month")["customer_id"]
        .nunique()
        .reset_index(name="churned_customers")
    )

    result = monthly_active.merge(churned, on="month", how="left")
    result["churned_customers"] = result["churned_customers"].fillna(0)
    result["churn_rate"] = (
        result["churned_customers"] / result["active_customers"]
    )

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold.customer_churn_monthly"))

        result.to_sql(
            name="customer_churn_monthly",
            schema="gold",
            con=conn,
            index=False,
            if_exists="replace",
        )

    print("✅ gold.customer_churn_monthly built successfully")


def build_gold_dau_mau():
    engine = get_engine()

    query = """
        SELECT
            user_id,
            event_date
        FROM silver.usage_events
        WHERE event_count > 0
    """

    df = pd.read_sql(query, engine)

    df["event_date"] = pd.to_datetime(df["event_date"])
    df["month"] = df["event_date"].dt.to_period("M").dt.to_timestamp()

    # DAU: distinct users per day
    daily_active = (
        df.groupby("event_date")["user_id"]
        .nunique()
        .reset_index(name="dau")
    )

    daily_active["month"] = daily_active["event_date"].dt.to_period("M").dt.to_timestamp()

    avg_dau = (
        daily_active.groupby("month")["dau"]
        .mean()
        .reset_index(name="dau")
    )

    # MAU: distinct users per month
    mau = (
        df.groupby("month")["user_id"]
        .nunique()
        .reset_index(name="mau")
    )

    result = avg_dau.merge(mau, on="month")
    result["dau_mau_ratio"] = result["dau"] / result["mau"]

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold.dau_mau_monthly"))

        result.to_sql(
            name="dau_mau_monthly",
            schema="gold",
            con=conn,
            index=False,
            if_exists="replace",
        )

    print("✅ gold.dau_mau_monthly built successfully")

def build_gold_active_customers():
    engine = get_engine()

    query = """
        SELECT
            customer_id,
            start_date,
            end_date
        FROM silver.subscriptions
    """

    df = pd.read_sql(query, engine)

    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    rows = []

    for _, row in df.iterrows():
        start = row["start_date"].replace(day=1)
        end = (
            row["end_date"].replace(day=1)
            if pd.notnull(row["end_date"])
            else start
        )

        for month in pd.date_range(start, end, freq="MS"):
            rows.append(
                {
                    "customer_id": row["customer_id"],
                    "month": month,
                }
            )

    activity = pd.DataFrame(rows)

    # Active customers per month
    monthly_active = (
        activity.groupby("month")["customer_id"]
        .nunique()
        .reset_index(name="active_customers")
    )

    # First appearance (new customers)
    first_seen = (
        activity.groupby("customer_id")["month"]
        .min()
        .reset_index(name="first_month")
    )

    new_customers = (
        first_seen.groupby("first_month")["customer_id"]
        .nunique()
        .reset_index(name="new_customers")
        .rename(columns={"first_month": "month"})
    )

    # Churned customers (reuse churn logic)
    activity["prev_month"] = activity["month"] + pd.offsets.MonthBegin(1)

    churn = (
        activity.merge(
            activity,
            left_on=["customer_id", "prev_month"],
            right_on=["customer_id", "month"],
            how="left",
            suffixes=("", "_next"),
        )
        .query("month_next.isna()")
    )

    churned = (
        churn.groupby("month")["customer_id"]
        .nunique()
        .reset_index(name="churned_customers")
    )

    # Combine
    result = (
        monthly_active
        .merge(new_customers, on="month", how="left")
        .merge(churned, on="month", how="left")
        .fillna(0)
    )

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold.active_customers_monthly"))

        result.to_sql(
            name="active_customers_monthly",
            schema="gold",
            con=conn,
            index=False,
            if_exists="replace",
        )

    print("✅ gold.active_customers_monthly built successfully")

def build_gold_dashboard_monthly():
    engine = get_engine()

    query = """
        SELECT
            m.month,
            m.mrr,
            a.active_customers,
            c.churn_rate,
            d.dau_mau_ratio
        FROM gold.mrr_monthly m
        LEFT JOIN gold.active_customers_monthly a USING (month)
        LEFT JOIN gold.customer_churn_monthly c USING (month)
        LEFT JOIN gold.dau_mau_monthly d USING (month)
    """

    df = pd.read_sql(query, engine)

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold.dashboard_monthly"))

        df.to_sql(
            name="dashboard_monthly",
            schema="gold",
            con=conn,
            index=False,
            if_exists="replace"
        )

    print("✅ gold.dashboard_monthly built successfully")




if __name__ == "__main__":
    build_gold_mrr_monthly()
    build_gold_customer_churn()
    build_gold_dau_mau()
    build_gold_active_customers()
    build_gold_dashboard_monthly()
