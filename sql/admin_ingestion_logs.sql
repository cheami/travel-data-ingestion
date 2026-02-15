DROP TABLE IF EXISTS admin.ingestion_logs;

CREATE TABLE IF NOT EXISTS admin.ingestion_logs (
    load_id NUMBER IDENTITY(1,1) PRIMARY KEY,
    file_id NUMBER,
    dataset_name VARCHAR(50),
    file_name VARCHAR(255),
    file_size_bytes NUMBER,
    file_type VARCHAR(20),
    row_count NUMBER,
    status VARCHAR(20),
    error_message STRING,
    ingestion_timestamp TIMESTAMP_TZ,
    target_schema VARCHAR(50),
    target_table VARCHAR(100)
);