CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.rejected_rows (
    table_name      TEXT,
    rule_name       TEXT,
    rejected_reason TEXT,
    row_data        JSONB,
    rejected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
