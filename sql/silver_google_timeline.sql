CREATE TABLE IF NOT EXISTS silver.google_timeline (
    timeline_id INT IDENTITY(1,1) PRIMARY KEY,
    load_id INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    segment_type VARCHAR(50),
    place_id VARCHAR(255),
    visit_latitude DECIMAL(10, 7),
    visit_longitude DECIMAL(10, 7),
    activity_type VARCHAR(100),
    activity_start_latitude DECIMAL(10, 7),
    activity_start_longitude DECIMAL(10, 7),
    activity_end_latitude DECIMAL(10, 7),
    activity_end_longitude DECIMAL(10, 7),
    distance_meters DECIMAL(10, 2),
    confidence DECIMAL(5, 4)
);