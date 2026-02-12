DROP TABLE IF EXISTS silver.manual_logs;

CREATE TABLE silver.manual_logs (
    -- Adjust based on actual CSV columns
    date TIMESTAMP,
    message TEXT,
    -- Metadata
    load_id INTEGER,
    row_id VARCHAR(255),
    _ingestion_time TIMESTAMP,
    _source_file VARCHAR(255)
);