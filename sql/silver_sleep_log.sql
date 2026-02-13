DROP TABLE IF EXISTS silver.sleep_log;

CREATE TABLE silver.sleep_log (
    sleep_log_entry_id BIGINT,
    sleep_start_time TIMESTAMP,
    sleep_date DATE,
    day_of_week VARCHAR(20),
    is_weekend BOOLEAN,
    overall_score INTEGER,
    composition_score FLOAT,
    revitalization_score INTEGER,
    duration_score FLOAT,
    deep_sleep_in_minutes INTEGER,
    resting_heart_rate INTEGER,
    restlessness FLOAT,
    sleep_quality_category VARCHAR(20),
    has_detailed_breakdown BOOLEAN,
    load_id INTEGER,
    row_id VARCHAR(255),
    _ingestion_time TIMESTAMP,
    _source_file VARCHAR(255)
);