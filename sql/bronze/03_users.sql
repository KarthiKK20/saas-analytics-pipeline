DROP TABLE IF EXISTS bronze.users;

CREATE TABLE bronze.users (
    user_id     TEXT,
    customer_id TEXT,
    user_role   TEXT,
    email       TEXT,
    created_at  TEXT,   -- raw string date
    is_active   TEXT    -- raw boolean-like value
);
