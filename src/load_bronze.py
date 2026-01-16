import os
import pandas as pd
from sqlalchemy import text

from config.db import get_engine


# Absolute path to bronze_inputs folder
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
BRONZE_INPUTS_DIR = os.path.join(PROJECT_ROOT, "bronze_inputs")


# Mapping: CSV file -> bronze table
FILE_TABLE_MAP = {
    "saas_bronze_raw_data-customers.csv": "bronze.customers",
    "saas_bronze_raw_data-users.csv": "bronze.users",
    "saas_bronze_raw_data-subscriptions.csv": "bronze.subscriptions",
    "saas_bronze_raw_data-payments.csv": "bronze.payments",
    "saas_bronze_raw_data-usage_events.csv": "bronze.usage_events",
}


def load_csv_to_bronze():
    """
    Loads all CSV files from bronze_inputs into bronze tables.
    This step is idempotent: tables are truncated before load.
    """
    engine = get_engine()

    for csv_file, table_name in FILE_TABLE_MAP.items():
        csv_path = os.path.join(BRONZE_INPUTS_DIR, csv_file)

        print(f"\nLoading file: {csv_file}")
        print(f"Target table: {table_name}")

        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"Rows read from CSV: {len(df)}")

        with engine.begin() as connection:
            # Truncate table before load (idempotent)
            connection.execute(text(f"TRUNCATE TABLE {table_name};"))

            # Load data
            df.to_sql(
                name=table_name.split(".")[1],
                schema="bronze",
                con=connection,
                if_exists="append",
                index=False
            )

        print(f"Loaded {len(df)} rows into {table_name}")


if __name__ == "__main__":
    load_csv_to_bronze()
