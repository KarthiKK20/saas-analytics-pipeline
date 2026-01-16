DROP TABLE IF EXISTS bronze.usage_events;

CREATE TABLE bronze.usage_events (
    event_id        TEXT,
    user_id         TEXT,
    event_type      TEXT,
    event_date TEXT,
    event_count  TEXT
);
