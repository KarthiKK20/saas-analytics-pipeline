DROP TABLE IF EXISTS bronze.payments;

CREATE TABLE bronze.payments (
    payment_id        TEXT,
    customer_id       TEXT,
    subscription_id   TEXT,
    payment_date      TEXT,
    amount    TEXT,
    payment_method    TEXT,
    payment_status    TEXT
);
