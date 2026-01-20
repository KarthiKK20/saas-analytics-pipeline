import json
import pandas as pd
from sqlalchemy import text
from config.db import get_engine

def write_rejected_rows(table_name, rule_name, reason, df):
    if df is None or df.empty:
        return

    engine = get_engine()

    records = [
        {
            "table_name": table_name,
            "rule_name": rule_name,
            "rejected_reason": reason,
            "row_data": json.dumps(
                {k: (str(v) if v is not None else None) for k, v in row.items()}
            ),
        }
        for row in df.to_dict(orient="records")
    ]

    audit_df = pd.DataFrame(records)

    with engine.begin() as conn:
        audit_df.to_sql(
            name="rejected_rows",
            schema="audit",
            con=conn,
            index=False,
            if_exists="append",
        )
