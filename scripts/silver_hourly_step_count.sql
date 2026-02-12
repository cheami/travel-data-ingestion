DROP TABLE IF EXISTS silver.hourly_step_count;

CREATE TABLE silver.hourly_step_count (
    date DATE,
    hour INTEGER,
    steps INTEGER,
    load_id INTEGER,
    _ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);