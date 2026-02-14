import pandas as pd
import json
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end

def parse_lat_lng(lat_lng_str):
    """Parses '27.9142°, -82.7040°' into (27.9142, -82.7040)."""
    if not lat_lng_str or not isinstance(lat_lng_str, str):
        return None, None
    try:
        # Remove degree symbol and whitespace
        parts = lat_lng_str.replace('°', '').strip().split(',')
        if len(parts) == 2:
            return float(parts[0].strip()), float(parts[1].strip())
    except ValueError:
        pass
    return None, None

def process_google_timeline(datasets_config, engine, hook, load_id=None):
    print("Processing Google Timeline...")
    
    timeline_table = "google_timeline" 
    
    try:
        # Determine load_ids to process
        if load_id:
            load_ids = [load_id]
        else:
            # Get recent load_ids from bronze that are not in silver
            # (Simplified logic: just processing specific load_id if provided, or all)
            # In production, you'd query for unprocessed IDs.
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{timeline_table}", engine)
            load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            trans_id = log_transformation_start(hook, load_id, 'google_timeline', 'google_timeline')
            try:
                # Read raw data. Assuming the JSON is in a column named 'data' or similar.
                # Adjust column name based on actual bronze table schema.
                df_raw = pd.read_sql(f"SELECT * FROM bronze.{timeline_table} WHERE load_id = {load_id}", engine)
                
                if df_raw.empty:
                    print(f"No data found for load_id {load_id}")
                    log_transformation_end(hook, trans_id, 'SUCCESS', 0)
                    continue

                # Dynamically find the JSON column if 'data' doesn't exist
                json_col = 'data'
                if 'data' not in df_raw.columns:
                    # Look for the first object/string column that isn't load_id
                    possible_cols = [c for c in df_raw.columns if c != 'load_id' and df_raw[c].dtype == 'object']
                    if possible_cols:
                        json_col = possible_cols[0]
                        print(f"Column 'data' not found. Using '{json_col}' as JSON source.")
                    else:
                        print(f"Error: Could not identify JSON column in {df_raw.columns}")
                        log_transformation_end(hook, trans_id, 'FAILURE', 0, "No JSON column found")
                        continue

                parsed_rows = []
                
                # Iterate through rows in bronze table
                for _, row in df_raw.iterrows():
                    raw_content = row[json_col]
                    if raw_content is None:
                        continue

                    # Handle if data is string/bytes (parse it) or already dict (jsonb)
                    if isinstance(raw_content, (str, bytes)):
                        json_content = json.loads(raw_content)
                    else:
                        json_content = raw_content
                    
                    if not json_content or not isinstance(json_content, dict) or 'semanticSegments' not in json_content:
                        continue

                    segments = json_content['semanticSegments']
                    
                    for segment in segments:
                        parsed_row = {
                            'load_id': load_id,
                            'start_time': segment.get('startTime'),
                            'end_time': segment.get('endTime'),
                            'segment_type': None,
                            'place_id': None,
                            'visit_latitude': None,
                            'visit_longitude': None,
                            'activity_type': None,
                            'activity_start_latitude': None,
                            'activity_start_longitude': None,
                            'activity_end_latitude': None,
                            'activity_end_longitude': None,
                            'distance_meters': None,
                            'confidence': None
                        }

                        # Handle Visit
                        if 'visit' in segment:
                            parsed_row['segment_type'] = 'VISIT'
                            visit = segment['visit']
                            parsed_row['confidence'] = visit.get('probability')
                            
                            if 'topCandidate' in visit:
                                candidate = visit['topCandidate']
                                parsed_row['place_id'] = candidate.get('placeId')
                                # If confidence wasn't at root, try candidate
                                if parsed_row['confidence'] is None:
                                    parsed_row['confidence'] = candidate.get('probability')

                                lat, lng = parse_lat_lng(candidate.get('placeLocation', {}).get('latLng'))
                                parsed_row['visit_latitude'] = lat
                                parsed_row['visit_longitude'] = lng

                        # Handle Activity
                        elif 'activity' in segment:
                            parsed_row['segment_type'] = 'ACTIVITY'
                            activity = segment['activity']
                            parsed_row['distance_meters'] = activity.get('distanceMeters')
                            
                            start_lat, start_lng = parse_lat_lng(activity.get('start', {}).get('latLng'))
                            end_lat, end_lng = parse_lat_lng(activity.get('end', {}).get('latLng'))
                            
                            parsed_row['activity_start_latitude'] = start_lat
                            parsed_row['activity_start_longitude'] = start_lng
                            parsed_row['activity_end_latitude'] = end_lat
                            parsed_row['activity_end_longitude'] = end_lng
                            
                            if 'topCandidate' in activity:
                                parsed_row['activity_type'] = activity['topCandidate'].get('type')
                                parsed_row['confidence'] = activity['topCandidate'].get('probability')
                        
                        # Only append if we identified a valid segment type (skips raw path segments)
                        if parsed_row['segment_type']:
                            parsed_rows.append(parsed_row)

                
                if parsed_rows:
                    df_silver = pd.DataFrame(parsed_rows)
                    # Convert timestamps
                    df_silver['start_time'] = pd.to_datetime(df_silver['start_time'], errors='coerce')
                    df_silver['end_time'] = pd.to_datetime(df_silver['end_time'], errors='coerce')
                    
                    print(f"Saving {len(df_silver)} rows to silver.google_timeline")
                    save_idempotent(df_silver, 'google_timeline', hook, engine)
                    log_transformation_end(hook, trans_id, 'SUCCESS', len(df_silver))
                else:
                    print("No valid Visit or Activity segments found.")
                    log_transformation_end(hook, trans_id, 'SUCCESS', 0)

            except Exception as e:
                print(f"Error processing load_id {load_id}: {e}")
                log_transformation_end(hook, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Error processing Google Timeline: {e}")
        raise e