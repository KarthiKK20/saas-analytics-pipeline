import pandas as pd
from sqlalchemy import text

from config.db import get_engine
from src.dq_utils import write_rejected_rows



def build_silver_customers():
    engine = get_engine()

    # Read from bronze
    query = """
        SELECT
            customer_id,
            customer_name,
            industry,
            country,
            signup_date,
            plan_type
        FROM bronze.customers
    """

    df = pd.read_sql(query, engine)

    # Convert signup_date to proper date
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    # Derived column useful for analytics
    df["signup_year"] = df["signup_date"].dt.year

    with engine.begin() as conn:
        # Recreate table (idempotent)
        conn.execute(text("DROP TABLE IF EXISTS silver.customers"))

        df.to_sql(
            name="customers",
            schema="silver",
            con=conn,
            index=False,
            if_exists="replace"
        )
    duplicates = df[df.duplicated("customer_id", keep=False)]

    write_rejected_rows(
        table_name="silver.customers",
        rule_name="PK_UNIQUENESS",
        reason="Duplicate customer_id found",
        df=duplicates
    )
    print("✅ silver.customers built successfully")



def build_silver_users():
    engine = get_engine()

    query = """
        SELECT
            user_id,
            customer_id,
            user_role,
            email,
            created_at,
            is_active
        FROM bronze.users
    """

    df = pd.read_sql(query, engine)

    # Parse created_at date
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # Normalize boolean
    df["is_active"] = (
    df["is_active"]
    .astype(str)
    .str.lower()
    .map({"true": True, "false": False})
)

    # Derived column
    df["created_year"] = df["created_at"].dt.year

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver.users"))

        df.to_sql(
            name="users",
            schema="silver",
            con=conn,
            index=False,
            if_exists="replace"
        )
    
    valid_customers = pd.read_sql("SELECT customer_id FROM silver.customers", engine)
    invalid_users = df[~df["customer_id"].isin(valid_customers["customer_id"])]

    write_rejected_rows(
        table_name="silver.users",
        rule_name="FK_CUSTOMER_EXISTS",
        reason="Invalid customer_id",
        df=invalid_users
    )

    print("✅ silver.users built successfully")


def build_silver_subscriptions():
    engine = get_engine()

    query = """
        SELECT
            subscription_id,
            customer_id,
            plan_name,
            start_date,
            end_date,
            monthly_amount,
            subscription_status
        FROM bronze.subscriptions
    """

    df = pd.read_sql(query, engine)

    # Parse dates
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    # Cast monthly_amount to numeric
    df["monthly_amount"] = pd.to_numeric(df["monthly_amount"], errors="coerce")

    # Active subscription flag
    df["is_active"] = df["subscription_status"].str.lower().eq("active")

    # Derived column
    df["start_year"] = df["start_date"].dt.year

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver.subscriptions"))

        df.to_sql(
            name="subscriptions",
            schema="silver",
            con=conn,
            index=False,
            if_exists="replace"
        )
    invalid_amounts = df[df["monthly_amount"] < 0]

    write_rejected_rows(
        table_name="silver.subscriptions",
        rule_name="AMOUNT_POSITIVE",
        reason="monthly_amount is negative",
        df=invalid_amounts
    )

    print("✅ silver.subscriptions built successfully")



def build_silver_payments():
    engine = get_engine()

    query = """
    SELECT
        payment_id,
        customer_id,
        subscription_id,
        payment_date,
        amount AS payment_amount,
        payment_method,
        payment_status
    FROM bronze.payments
"""


    df = pd.read_sql(query, engine)

    # Parse payment_date
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")

    # Cast payment_amount
    df["payment_amount"] = pd.to_numeric(df["payment_amount"], errors="coerce")

    # Derived column
    df["payment_year"] = df["payment_date"].dt.year

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver.payments"))

        df.to_sql(
            name="payments",
            schema="silver",
            con=conn,
            index=False,
            if_exists="replace"
        )

    invalid_payments = df[df["payment_amount"] <= 0]

    write_rejected_rows(
        table_name="silver.payments",
        rule_name="PAYMENT_AMOUNT_POSITIVE",
        reason="payment_amount is zero or negative",
        df=invalid_payments
    )

    print("✅ silver.payments built successfully")

def build_silver_usage_events():
    engine = get_engine()

    query = """
        SELECT
            event_id,
            user_id,
            event_type,
            event_date,
            event_count
        FROM bronze.usage_events
    """

    df = pd.read_sql(query, engine)

    # Parse event_date
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date

    # Cast event_count to integer
    df["event_count"] = pd.to_numeric(df["event_count"], errors="coerce")

    # Derived column
    df["event_year"] = pd.DatetimeIndex(df["event_date"]).year

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS silver.usage_events"))

        df.to_sql(
            name="usage_events",
            schema="silver",
            con=conn,
            index=False,
            if_exists="replace"
        )
    invalid_events = df[df["event_count"] < 0]

    write_rejected_rows(
        table_name="silver.usage_events",
        rule_name="EVENT_COUNT_NON_NEGATIVE",
        reason="event_count is negative",
        df=invalid_events
    )

    print("✅ silver.usage_events built successfully")


if __name__ == "__main__":
    build_silver_customers()
    build_silver_users()
    build_silver_subscriptions()
    build_silver_payments()
    build_silver_usage_events()
