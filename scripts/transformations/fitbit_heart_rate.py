import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end, check_data_exists

def get_heart_rate_zone(bpm):
    if pd.isna(bpm): return 'Unknown'
    if bpm < 60: return 'Resting'
    if bpm < 100: return 'Normal'
    if bpm < 130: return 'Elevated'
    return 'High'

def process_fitbit_heart_rate(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Fitbit Heart Rate...")
    hr_config = datasets_config.get('fitbit_heart_rate', {})
    hr_table = hr_config.get('target_table', 'fitbit_heart_rate')

    try:
        # get load ids
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{hr_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        # filter processed
        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE DATASET_NAME = 'fitbit_heart_rate' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for fitbit_heart_rate.")

        for load_id in load_ids:
            if not check_data_exists(conn, load_id, 'bronze', hr_table):
                print(f"Skipping load_id {load_id} for fitbit_heart_rate (no data in bronze).")
                continue

            trans_id = log_transformation_start(conn, load_id, 'fitbit_heart_rate', 'heart_rate_minute_log, heart_rate_hourly_summary')
            try:
                # read data
                df_hr = pd.read_sql(f"SELECT * FROM bronze.{hr_table} WHERE load_id = {load_id}", conn)
                
                if df_hr.empty:
                    log_transformation_end(conn, trans_id, 'SUCCESS', 0)
                    continue

                df_hr.columns = df_hr.columns.str.strip().str.lower()
                
                # find time and value cols
                time_col = 'timestamp' if 'timestamp' in df_hr.columns else next((c for c in df_hr.columns if 'time' in c), 'timestamp')
                val_col = next((c for c in df_hr.columns if 'value' in c or 'rate' in c or 'bpm' in c or 'beats' in c), None)

                if not val_col:
                    raise ValueError(f"Column not found: value. Available columns: {df_hr.columns.tolist()}")

                # normalize time
                df_hr[time_col] = pd.to_datetime(df_hr[time_col])
                df_hr['log_timestamp'] = df_hr[time_col].dt.floor('min')
                
                # agg by minute
                minute_agg = df_hr.groupby(['log_timestamp', 'load_id'])[val_col].agg(['mean', 'min', 'max', 'count']).reset_index()
                minute_agg.columns = ['log_timestamp', 'load_id', 'heart_rate_mean', 'heart_rate_min', 'heart_rate_max', 'readings_count']
                
                # calc zone
                minute_agg['hr_zone'] = minute_agg['heart_rate_mean'].apply(get_heart_rate_zone)
                
                # agg hourly
                minute_agg['date'] = minute_agg['log_timestamp'].dt.date
                minute_agg['hour'] = minute_agg['log_timestamp'].dt.hour

                hourly_agg = minute_agg.groupby(['date', 'hour', 'load_id']).agg(
                    hourly_avg_hr=('heart_rate_mean', 'mean'),
                    hourly_min_hr=('heart_rate_min', 'min'),
                    hourly_max_hr=('heart_rate_max', 'max'),
                    minutes_in_resting=('hr_zone', lambda x: (x == 'Resting').sum()),
                    minutes_in_elevated=('hr_zone', lambda x: (x == 'Elevated').sum()),
                    minutes_in_high=('hr_zone', lambda x: (x == 'High').sum())
                ).reset_index()

                # cleanup minute agg
                minute_agg = minute_agg[['log_timestamp', 'load_id', 'heart_rate_mean', 'heart_rate_min', 'heart_rate_max', 'readings_count', 'hr_zone']]
                
                # stringify timestamp
                minute_agg['log_timestamp'] = minute_agg['log_timestamp'].astype(str)

                # save
                save_idempotent(minute_agg, 'heart_rate_minute_log', conn)
                save_idempotent(hourly_agg, 'heart_rate_hourly_summary', conn)

                # log success
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM SILVER.HEART_RATE_MINUTE_LOG WHERE load_id = {load_id}")
                row_count = cursor.fetchone()[0]
                cursor.close()
                log_transformation_end(conn, trans_id, 'SUCCESS', row_count)

            except Exception as e:
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping fitbit heart rate: {e}")