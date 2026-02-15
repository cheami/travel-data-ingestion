CREATE TABLE admin.file_details (
    file_id INT IDENTITY(1,1),
    container VARCHAR(50),
    stage VARCHAR(50),
    source_path VARCHAR(255),
    file_pattern VARCHAR(50),
    target_schema VARCHAR(50),
    target_table VARCHAR(100),
    format VARCHAR(20)
);