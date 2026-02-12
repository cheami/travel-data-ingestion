DROP TABLE IF EXISTS silver.all_spending;

CREATE TABLE silver.all_spending (
    date DATE,
    type VARCHAR(255),
    amount NUMERIC,
    -- Common transaction columns (adjust if your CSV has different names)
    description TEXT,
    category VARCHAR(255),
    -- Metadata
    load_id INTEGER,
    row_id VARCHAR(255),
    _ingestion_time TIMESTAMP,
    _source_file VARCHAR(255)
);