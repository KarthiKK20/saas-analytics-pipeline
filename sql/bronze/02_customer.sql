DROP TABLE IF EXISTS bronze.customers;

CREATE TABLE bronze.customers (
    customer_id   TEXT,
    customer_name TEXT,
    industry      TEXT,
    country       TEXT,
    signup_date   TEXT,   -- RAW, unparsed
    plan_type     TEXT
);
