import pandas as pd
from transformations.utils import save_idempotent

def process_fitbit_steps(datasets_config, engine, hook):
    print("Processing Fitbit Steps...")
    steps_config = datasets_config.get('fitbit_steps', {})
    steps_table = steps_config.get('target_table', 'fitbit_steps')

    try:
        load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{steps_table}", engine)
        load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            df_steps = pd.read_sql(f"SELECT * FROM bronze.{steps_table} WHERE load_id = {load_id}", engine)
            if not df_steps.empty:
                # Convert timestamp to datetime
                df_steps['timestamp'] = pd.to_datetime(df_steps['timestamp'])
                df_steps['date'] = df_steps['timestamp'].dt.date
                df_steps['hour'] = df_steps['timestamp'].dt.hour

                # Aggregate steps by date and hour
                # We take the max load_id for the group to maintain lineage
                hourly_agg = df_steps.groupby(['date', 'hour']).agg({'steps': 'sum', 'load_id': 'max'}).reset_index()

                # Ensure 24 rows for every date (0-23 hours)
                dates = hourly_agg['date'].unique()
                all_combinations = [{'date': d, 'hour': h} for d in dates for h in range(24)]
                df_full = pd.DataFrame(all_combinations)

                # Merge aggregated data with the full 24-hour frame
                df_final = pd.merge(df_full, hourly_agg, on=['date', 'hour'], how='left')

                # Fill missing steps with 0
                df_final['steps'] = df_final['steps'].fillna(0).astype(int)

                # Fill missing load_id with the max load_id for that date (so they are grouped correctly)
                date_load_map = df_steps.groupby('date')['load_id'].max().to_dict()
                df_final['load_id'] = df_final['load_id'].fillna(df_final['date'].map(date_load_map)).astype(int)

                save_idempotent(df_final, 'hourly_step_count', hook, engine)

    except Exception as e:
        print(f"Skipping fitbit steps: {e}")