DROP TABLE IF EXISTS silver.sleep_daily_summary;

CREATE TABLE silver.sleep_daily_summary (
    sleep_date DATE,
    avg_overall_score FLOAT,
    avg_resting_heart_rate FLOAT,
    total_deep_sleep_minutes INTEGER,
    sleep_session_count INTEGER,
    load_id INTEGER,
    _ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);