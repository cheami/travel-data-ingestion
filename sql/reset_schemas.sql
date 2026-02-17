USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

--DROP SCHEMA IF EXISTS ADMIN CASCADE;
--DROP SCHEMA IF EXISTS BRONZE CASCADE;
--DROP SCHEMA IF EXISTS SILVER CASCADE;
--DROP SCHEMA IF EXISTS GOLD CASCADE;
--
--CREATE SCHEMA ADMIN;
--CREATE SCHEMA BRONZE;
--CREATE SCHEMA SILVER;
--CREATE SCHEMA GOLD;

create or replace TABLE travel_data.admin.ingestion_logs (
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

create or replace TABLE travel_data.ADMIN.TRANSFORMATION_LOGS (
    TRANSFORMATION_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
    LOAD_ID NUMBER,
    DATASET_NAME VARCHAR(100),
    TARGET_TABLE VARCHAR(100),
    STATUS VARCHAR(20),
    ROWS_PROCESSED NUMBER,
    ERROR_MESSAGE STRING,
    START_TIME TIMESTAMP_NTZ(9),
    END_TIME TIMESTAMP_NTZ(9)
);

--create or replace TABLE travel_data.admin.file_details (
--    file_id INT IDENTITY(1,1),
--    container VARCHAR(50),
--    stage VARCHAR(50),
--    source_path VARCHAR(255),
--    file_pattern VARCHAR(50),
--    target_schema VARCHAR(50),
--    target_table VARCHAR(100),
--    format VARCHAR(20)
--);


CREATE OR REPLACE FILE FORMAT BRONZE.CSV
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
COMPRESSION = 'AUTO';

CREATE OR REPLACE FILE FORMAT BRONZE.json
TYPE = 'JSON';


create or replace TABLE TRAVEL_DATA.BRONZE.FITBIT_HEART_RATE (
	TIMESTAMP VARCHAR(16777216),
	BEATS_PER_MINUTE FLOAT,
	DATA_SOURCE VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);

create or replace TABLE TRAVEL_DATA.BRONZE.FITBIT_SLEEP_SCORE (
	SLEEP_LOG_ENTRY_ID NUMBER(38,0),
	TIMESTAMP VARCHAR(16777216),
	OVERALL_SCORE NUMBER(38,0),
	COMPOSITION_SCORE FLOAT,
	REVITALIZATION_SCORE NUMBER(38,0),
	DURATION_SCORE FLOAT,
	DEEP_SLEEP_IN_MINUTES NUMBER(38,0),
	RESTING_HEART_RATE NUMBER(38,0),
	RESTLESSNESS FLOAT,
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);

create or replace TABLE TRAVEL_DATA.BRONZE.FITBIT_STEPS (
	TIMESTAMP VARCHAR(16777216),
	STEPS NUMBER(38,0),
	DATA_SOURCE VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);

create or replace TABLE TRAVEL_DATA.BRONZE.FLIGHT_LOGS (
	DATE VARCHAR(16777216),
	FLIGHT_NUMBER VARCHAR(16777216),
	"FROM" VARCHAR(16777216),
	"TO" VARCHAR(16777216),
	DEP_TIME VARCHAR(16777216),
	ARR_TIME VARCHAR(16777216),
	DURATION VARCHAR(16777216),
	AIRLINE VARCHAR(16777216),
	AIRCRAFT VARCHAR(16777216),
	REGISTRATION VARCHAR(16777216),
	SEAT_NUMBER VARCHAR(16777216),
	SEAT_TYPE VARCHAR(16777216),
	FLIGHT_CLASS VARCHAR(16777216),
	FLIGHT_REASON VARCHAR(16777216),
	NOTE VARCHAR(16777216),
	DEP_ID VARCHAR(16777216),
	ARR_ID VARCHAR(16777216),
	AIRLINE_ID VARCHAR(16777216),
	AIRCRAFT_ID VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);

create or replace TABLE TRAVEL_DATA.BRONZE.GOOGLE_TIMELINE (
	RAW_DATA VARIANT,
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);

create or replace TABLE TRAVEL_DATA.BRONZE.MANUAL_LOGS (
	DAY NUMBER(38,0),
	DATE VARCHAR(16777216),
	FLAG FLOAT,
	COUNTY VARCHAR(16777216),
	CITY VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	COMMENTS VARCHAR(16777216),
	FOOD VARCHAR(16777216),
	TRAVEL VARCHAR(16777216),
	HOTEL VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);

create or replace TABLE TRAVEL_DATA.BRONZE.TRANSACTIONS (
	COUNTRY VARCHAR(16777216),
	DATE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	AMOUNT VARCHAR(16777216),
	COMMENTS VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID NUMBER IDENTITY(1,1) PRIMARY KEY
);



create or replace TABLE TRAVEL_DATA.SILVER.ALL_SPENDING (
	COUNTRY VARCHAR(16777216),
	DATE VARCHAR(16777216),
	NAME VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	AMOUNT FLOAT,
	COMMENTS VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID VARCHAR(16777216)
);

create or replace TABLE TRAVEL_DATA.SILVER.DAILY_SPEND (
	DATE VARCHAR(16777216),
	TYPE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	AMOUNT FLOAT
);

create or replace TABLE TRAVEL_DATA.SILVER.FLIGHT_LOGS (
	DATE VARCHAR(16777216),
	FLIGHT_NUMBER VARCHAR(16777216),
	"FROM" VARCHAR(16777216),
	"TO" VARCHAR(16777216),
	DEP_TIME VARCHAR(16777216),
	ARR_TIME VARCHAR(16777216),
	DURATION VARCHAR(16777216),
	AIRLINE VARCHAR(16777216),
	AIRCRAFT VARCHAR(16777216),
	REGISTRATION VARCHAR(16777216),
	SEAT_NUMBER VARCHAR(16777216),
	SEAT_TYPE NUMBER(38,0),
	FLIGHT_CLASS NUMBER(38,0),
	FLIGHT_REASON NUMBER(38,0),
	NOTE VARCHAR(16777216),
	DEP_ID NUMBER(38,0),
	ARR_ID NUMBER(38,0),
	AIRLINE_ID NUMBER(38,0),
	AIRCRAFT_ID NUMBER(38,0),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID VARCHAR(16777216)
);

create or replace TABLE TRAVEL_DATA.SILVER.GOOGLE_TIMELINE (
	TIMELINE_ID NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
	LOAD_ID NUMBER(38,0),
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	SEGMENT_TYPE VARCHAR(50),
	PLACE_ID VARCHAR(255),
	VISIT_LATITUDE NUMBER(10,7),
	VISIT_LONGITUDE NUMBER(10,7),
	ACTIVITY_TYPE VARCHAR(100),
	ACTIVITY_START_LATITUDE NUMBER(10,7),
	ACTIVITY_START_LONGITUDE NUMBER(10,7),
	ACTIVITY_END_LATITUDE NUMBER(10,7),
	ACTIVITY_END_LONGITUDE NUMBER(10,7),
	DISTANCE_METERS NUMBER(10,2),
	CONFIDENCE NUMBER(5,4),
	primary key (TIMELINE_ID)
);

create or replace TABLE TRAVEL_DATA.SILVER.HEART_RATE_HOURLY_SUMMARY (
	DATE VARCHAR(16777216),
	HOUR NUMBER(38,0),
	LOAD_ID NUMBER(38,0),
	HOURLY_AVG_HR FLOAT,
	HOURLY_MIN_HR FLOAT,
	HOURLY_MAX_HR FLOAT,
	MINUTES_IN_RESTING NUMBER(38,0),
	MINUTES_IN_ELEVATED NUMBER(38,0),
	MINUTES_IN_HIGH NUMBER(38,0)
);

create or replace TABLE TRAVEL_DATA.SILVER.HEART_RATE_MINUTE_LOG (
	LOG_TIMESTAMP VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	HEART_RATE_MEAN FLOAT,
	HEART_RATE_MIN FLOAT,
	HEART_RATE_MAX FLOAT,
	READINGS_COUNT NUMBER(38,0),
	HR_ZONE VARCHAR(16777216)
);

create or replace TABLE TRAVEL_DATA.SILVER.HOURLY_STEP_COUNT (
	DATE VARCHAR(16777216),
	HOUR NUMBER(38,0),
	STEPS NUMBER(38,0),
	LOAD_ID NUMBER(38,0)
);

create or replace TABLE TRAVEL_DATA.SILVER.MANUAL_LOGS (
	DAY NUMBER(38,0),
	DATE VARCHAR(16777216),
	FLAG VARCHAR(16777216),
	COUNTY VARCHAR(16777216),
	CITY VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	COMMENTS VARCHAR(16777216),
	FOOD VARCHAR(16777216),
	TRAVEL VARCHAR(16777216),
	HOTEL VARCHAR(16777216),
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID VARCHAR(16777216)
);

create or replace TABLE TRAVEL_DATA.SILVER.SLEEP_LOG (
	SLEEP_LOG_ENTRY_ID NUMBER(38,0),
	TIMESTAMP VARCHAR(16777216),
	OVERALL_SCORE NUMBER(38,0),
	COMPOSITION_SCORE FLOAT,
	REVITALIZATION_SCORE NUMBER(38,0),
	DURATION_SCORE FLOAT,
	DEEP_SLEEP_IN_MINUTES NUMBER(38,0),
	RESTING_HEART_RATE NUMBER(38,0),
	RESTLESSNESS FLOAT,
	_INGESTION_TIME VARCHAR(16777216),
	_SOURCE_FILE VARCHAR(16777216),
	LOAD_ID NUMBER(38,0),
	ROW_ID VARCHAR(16777216)
);





CREATE OR REPLACE PROCEDURE travel_data.SILVER.PROCESS_GOOGLE_TIMELINE(LOAD_ID_PARAM NUMBER(38,0))
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
import pandas as pd
import json

def parse_lat_long_string(geo_str):
    if not geo_str or not isinstance(geo_str, str):
        return None, None
    try:
        clean_str = geo_str.replace('°', '').strip()
        parts = clean_str.split(',')
        if len(parts) == 2:
            return float(parts[0].strip()), float(parts[1].strip())
    except:
        return None, None
    return None, None

def main(session, load_id_param):
    # 1. Determine which load_ids to process
    load_ids_to_process = []
    
    if load_id_param is not None:
        load_ids_to_process = [load_id_param]
    else:
        df_ids = session.table("bronze.google_timeline").select("load_id").distinct().collect()
        load_ids_to_process = [row['LOAD_ID'] for row in df_ids]

    if not load_ids_to_process:
        return "No load_ids found to process."

    processed_count = 0

    for current_load_id in load_ids_to_process:
        
        # --- FIXED LOGIC ---
        # Instead of fetching the huge 'raw_data' column (which causes the 16MB error),
        # we use SQL 'LATERAL FLATTEN' to break the JSON into small rows (segments)
        # and only fetch those into Python.
        
        # We assume raw_data is a VARIANT column. 
        # If it is a VARCHAR, use: PARSE_JSON(raw_data):semanticSegments
        sql_extract = f"""
            SELECT 
                value AS segment_json
            FROM bronze.google_timeline,
            LATERAL FLATTEN(input => raw_data:semanticSegments)
            WHERE load_id = {current_load_id}
        """
        
        # This returns a DataFrame where each row is ONE segment (small), not the whole file.
        segments_df = session.sql(sql_extract).to_pandas()
        
        if segments_df.empty:
            continue

        parsed_rows = []

        for _, row in segments_df.iterrows():
            # Get the individual segment data
            segment_data = row['SEGMENT_JSON']
            
            # Ensure it is a dict
            segment = json.loads(segment_data) if isinstance(segment_data, str) else segment_data
            
            # Extract time from the segment
            start_time = segment.get('startTime')
            end_time = segment.get('endTime')
            
            row_data = {
                'LOAD_ID': current_load_id,
                'START_TIME': start_time,
                'END_TIME': end_time,
                'SEGMENT_TYPE': None,
                'PLACE_ID': None,
                'VISIT_LATITUDE': None,
                'VISIT_LONGITUDE': None,
                'ACTIVITY_TYPE': None,
                'ACTIVITY_START_LATITUDE': None,
                'ACTIVITY_START_LONGITUDE': None,
                'ACTIVITY_END_LATITUDE': None,
                'ACTIVITY_END_LONGITUDE': None,
                'DISTANCE_METERS': None,
                'CONFIDENCE': None
            }

            if 'visit' in segment:
                row_data['SEGMENT_TYPE'] = 'VISIT'
                visit = segment['visit']
                row_data['CONFIDENCE'] = visit.get('probability')
                top_candidate = visit.get('topCandidate', {})
                row_data['PLACE_ID'] = top_candidate.get('placeId')
                
                loc_node = top_candidate.get('placeLocation')
                loc_str = None
                if isinstance(loc_node, dict):
                    loc_str = loc_node.get('latLng')
                elif isinstance(loc_node, str):
                    loc_str = loc_node
                
                lat, lng = parse_lat_long_string(loc_str)
                row_data['VISIT_LATITUDE'] = lat
                row_data['VISIT_LONGITUDE'] = lng
                
                parsed_rows.append(row_data)

            elif 'activity' in segment: 
                row_data['SEGMENT_TYPE'] = 'ACTIVITY'
                activity = segment['activity']
                top_candidate = activity.get('topCandidate', {})

                row_data['CONFIDENCE'] = activity.get('probability') or top_candidate.get('probability')
                row_data['DISTANCE_METERS'] = activity.get('distanceMeters')
                row_data['ACTIVITY_TYPE'] = top_candidate.get('type')
                
                start_node = activity.get('start') or activity.get('startLocation') or {}
                end_node = activity.get('end') or activity.get('endLocation') or {}
                
                s_lat, s_lng = parse_lat_long_string(start_node.get('latLng'))
                e_lat, e_lng = parse_lat_long_string(end_node.get('latLng'))
                
                row_data['ACTIVITY_START_LATITUDE'] = s_lat
                row_data['ACTIVITY_START_LONGITUDE'] = s_lng
                row_data['ACTIVITY_END_LATITUDE'] = e_lat
                row_data['ACTIVITY_END_LONGITUDE'] = e_lng
                
                parsed_rows.append(row_data)

        if parsed_rows:
            target_df = pd.DataFrame(parsed_rows)
            snowpark_df = session.create_dataframe(target_df)
            
            session.sql(f"DELETE FROM silver.google_timeline WHERE load_id = {current_load_id}").collect()
            
            staging_table_name = f"silver.staging_google_timeline_{current_load_id}"
            snowpark_df.write.save_as_table(staging_table_name, mode="overwrite")
            
            cols = [
                'LOAD_ID', 'START_TIME', 'END_TIME', 'SEGMENT_TYPE', 'PLACE_ID', 
                'VISIT_LATITUDE', 'VISIT_LONGITUDE', 'ACTIVITY_TYPE', 
                'ACTIVITY_START_LATITUDE', 'ACTIVITY_START_LONGITUDE', 
                'ACTIVITY_END_LATITUDE', 'ACTIVITY_END_LONGITUDE', 
                'DISTANCE_METERS', 'CONFIDENCE'
            ]
            cols_str = ", ".join(cols)
            
            insert_sql = f"""
                INSERT INTO silver.google_timeline ({cols_str}) 
                SELECT {cols_str} FROM {staging_table_name}
            """
            session.sql(insert_sql).collect()
            session.sql(f"DROP TABLE IF EXISTS {staging_table_name}").collect()
            
            processed_count += 1

    return f"Success: Processed {processed_count} load_ids."
$$;