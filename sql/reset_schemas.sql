DROP SCHEMA IF EXISTS admin CASCADE;
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

CREATE SCHEMA admin;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

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

CREATE TABLE IF NOT EXISTS admin.transformation_logs (
    transformation_id SERIAL PRIMARY KEY,
    load_id INTEGER,
    transformation_name VARCHAR(100),
    target_table VARCHAR(100),
    status VARCHAR(20),
    rows_processed INTEGER,
    error_message TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);