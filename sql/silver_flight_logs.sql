DROP TABLE IF EXISTS silver.flight_logs;

CREATE TABLE silver.flight_logs (
    -- Preserved columns from Bronze
    flight_date DATE,
    flight_number VARCHAR(50),
    
    -- Extracted Departure Columns
    departure_city VARCHAR(255),
    departure_airport VARCHAR(255),
    departure_airport_code_iata VARCHAR(10),
    departure_airport_code_icao VARCHAR(10),
    
    -- Extracted Arrival Columns
    arrival_city VARCHAR(255),
    arrival_airport VARCHAR(255),
    arrival_airport_code_iata VARCHAR(10),
    arrival_airport_code_icao VARCHAR(10),
    
    -- Extracted Airline Columns
    airline_name VARCHAR(255),
    airline_iata VARCHAR(10),
    airline_icao VARCHAR(10),
    
    -- Metadata
    load_id INTEGER,
    row_id VARCHAR(255),
    _ingestion_time TIMESTAMP,
    _source_file VARCHAR(255)
);