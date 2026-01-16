DROP TABLE IF EXISTS bronze.subscriptions;

CREATE TABLE bronze.subscriptions (
    subscription_id     TEXT,
    customer_id         TEXT,
    plan_name           TEXT,
    start_date          TEXT,
    end_date            TEXT,
    monthly_amount      TEXT,
    subscription_status TEXT
);
