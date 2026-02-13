DROP TABLE IF EXISTS silver.heart_rate_minute_log;

CREATE TABLE silver.heart_rate_minute_log (
    log_timestamp TIMESTAMP,
    heart_rate_mean FLOAT,
    heart_rate_min FLOAT,
    heart_rate_max FLOAT,
    readings_count INTEGER,
    hr_zone VARCHAR(20),
    load_id INTEGER,
    _ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);