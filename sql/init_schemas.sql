-- Create the new layer schemas
CREATE SCHEMA IF NOT EXISTS admin;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create the ingestion logs table in the admin schema
CREATE TABLE IF NOT EXISTS admin.ingestion_logs (
    load_id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(50),
    file_name VARCHAR(255),
    file_size_bytes BIGINT,
    file_type VARCHAR(20),
    row_count INT,
    status VARCHAR(20),
    error_message TEXT,
    ingestion_timestamp TIMESTAMP,
    target_schema VARCHAR(50),
    target_table VARCHAR(100)
);