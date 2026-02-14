DROP TABLE IF EXISTS admin.transformation_logs;

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