import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end, check_data_exists

def process_fitbit_steps(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Fitbit Steps...")
    steps_config = datasets_config.get('fitbit_steps', {})
    steps_table = steps_config.get('target_table', 'fitbit_steps')

    try:
        # get load ids
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{steps_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        # filter processed
        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE DATASET_NAME = 'fitbit_steps' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for fitbit_steps.")

        for load_id in load_ids:
            if not check_data_exists(conn, load_id, 'bronze', steps_table):
                print(f"Skipping load_id {load_id} for fitbit_steps (no data in bronze).")
                continue

            trans_id = log_transformation_start(conn, load_id, 'fitbit_steps', 'hourly_step_count')
            try:
                # read data
                df_steps = pd.read_sql(f"SELECT * FROM bronze.{steps_table} WHERE load_id = {load_id}", conn)
                
                if df_steps.empty:
                    log_transformation_end(conn, trans_id, 'SUCCESS', 0)
                    continue

                df_steps.columns = df_steps.columns.str.strip().str.lower()
                
                # fix dates
                df_steps['timestamp'] = pd.to_datetime(df_steps['timestamp'])
                df_steps['date'] = df_steps['timestamp'].dt.date
                df_steps['hour'] = df_steps['timestamp'].dt.hour

                # agg steps
                hourly_agg = df_steps.groupby(['date', 'hour']).agg({'steps': 'sum', 'load_id': 'max'}).reset_index()

                # fill 24h
                dates = hourly_agg['date'].unique()
                all_combinations = [{'date': d, 'hour': h} for d in dates for h in range(24)]
                df_full = pd.DataFrame(all_combinations)

                # merge and fill
                df_final = pd.merge(df_full, hourly_agg, on=['date', 'hour'], how='left')
                df_final['steps'] = df_final['steps'].fillna(0).astype(int)

                # fill load_id
                date_load_map = df_steps.groupby('date')['load_id'].max().to_dict()
                df_final['load_id'] = df_final['load_id'].fillna(df_final['date'].map(date_load_map)).astype(int)

                # save
                save_idempotent(df_final, 'hourly_step_count', conn)
                
                # log success
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM SILVER.HOURLY_STEP_COUNT WHERE load_id = {load_id}")
                row_count = cursor.fetchone()[0]
                cursor.close()
                log_transformation_end(conn, trans_id, 'SUCCESS', row_count)

            except Exception as e:
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping fitbit steps: {e}")