import pandas as pd
from transformations.utils import save_idempotent

def process_fitbit_heart_rate(datasets_config, engine, hook, load_id=None):
    print("Processing Fitbit Heart Rate...")
    hr_config = datasets_config.get('fitbit_heart_rate', {})
    hr_table = hr_config.get('target_table', 'fitbit_heart_rate')

    try:
        if load_id:
            load_ids = [load_id]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{hr_table}", engine)
            load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            df_hr = pd.read_sql(f"SELECT * FROM bronze.{hr_table} WHERE load_id = {load_id}", engine)
            if not df_hr.empty:
                df_hr['timestamp'] = pd.to_datetime(df_hr['timestamp'])
                
                # 1. Minute Level Aggregation
                # Group by minute and load_id to maintain lineage
                df_hr['minute_timestamp'] = df_hr['timestamp'].dt.floor('min')
                
                minute_agg = df_hr.groupby(['minute_timestamp', 'load_id']).agg(
                    heart_rate_mean=('beats per minute', 'mean'),
                    heart_rate_min=('beats per minute', 'min'),
                    heart_rate_max=('beats per minute', 'max'),
                    readings_count=('beats per minute', 'count')
                ).reset_index()
                
                def get_zone(hr):
                    if hr < 60: return 'Resting'
                    if hr <= 100: return 'Normal'
                    if hr <= 140: return 'Elevated'
                    return 'High'
                
                minute_agg['hr_zone'] = minute_agg['heart_rate_mean'].apply(get_zone)
                minute_agg.rename(columns={'minute_timestamp': 'log_timestamp'}, inplace=True)
                
                save_idempotent(minute_agg, 'heart_rate_minute_log', hook, engine)
                
                # 2. Hourly Aggregation
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
                
                hourly_agg['hourly_avg_hr'] = hourly_agg['hourly_avg_hr'].round(1)
                
                save_idempotent(hourly_agg, 'heart_rate_hourly_summary', hook, engine)

    except Exception as e:
        print(f"Skipping fitbit heart rate: {e}")