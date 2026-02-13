DROP TABLE IF EXISTS silver.heart_rate_hourly_summary;

CREATE TABLE silver.heart_rate_hourly_summary (
    date DATE,
    hour INTEGER,
    hourly_avg_hr FLOAT,
    hourly_min_hr FLOAT,
    hourly_max_hr FLOAT,
    minutes_in_resting INTEGER,
    minutes_in_elevated INTEGER,
    minutes_in_high INTEGER,
    load_id INTEGER,
    _ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);