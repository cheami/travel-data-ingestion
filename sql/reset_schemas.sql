DROP SCHEMA IF EXISTS ADMIN CASCADE;
DROP SCHEMA IF EXISTS BRONZE CASCADE;
DROP SCHEMA IF EXISTS SILVER CASCADE;
DROP SCHEMA IF EXISTS GOLD CASCADE;

CREATE SCHEMA ADMIN;
CREATE SCHEMA BRONZE;
CREATE SCHEMA SILVER;
CREATE SCHEMA GOLD;

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

CREATE TABLE IF NOT EXISTS ADMIN.TRANSFORMATION_LOGS (
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

CREATE TABLE IF NOT EXISTS admin.file_details (
    file_id INT IDENTITY(1,1),
    container VARCHAR(50),
    stage VARCHAR(50),
    source_path VARCHAR(255),
    file_pattern VARCHAR(50),
    target_schema VARCHAR(50),
    target_table VARCHAR(100),
    format VARCHAR(20)
);

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

CREATE OR REPLACE PROCEDURE PROCESS_GOOGLE_TIMELINE(LOAD_ID_PARAM NUMBER(38,0))
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