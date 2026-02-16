import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end, check_data_exists

def process_fitbit_sleep(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Fitbit Sleep...")
    sleep_config = datasets_config.get('fitbit_sleep', {})
    sleep_table = sleep_config.get('target_table', 'fitbit_sleep_score')

    try:
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{sleep_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE DATASET_NAME = 'fitbit_sleep' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for fitbit_sleep.")

        for load_id in load_ids:
            if not check_data_exists(conn, load_id, 'bronze', sleep_table):
                print(f"Skipping load_id {load_id} for fitbit_sleep (no data in bronze).")
                continue

            trans_id = log_transformation_start(conn, load_id, 'fitbit_sleep', 'sleep_log')
            try:
                df_sleep = pd.read_sql(f"SELECT * FROM bronze.{sleep_table} WHERE load_id = {load_id}", conn)
                
                if df_sleep.empty:
                    log_transformation_end(conn, trans_id, 'SUCCESS', 0)
                    continue

                df_sleep.columns = df_sleep.columns.str.strip().str.lower()

                # Ensure date/time types
                if 'dateofsleep' in df_sleep.columns:
                    df_sleep['dateofsleep'] = pd.to_datetime(df_sleep['dateofsleep']).dt.date
                if 'starttime' in df_sleep.columns:
                    df_sleep['starttime'] = pd.to_datetime(df_sleep['starttime'])
                if 'endtime' in df_sleep.columns:
                    df_sleep['endtime'] = pd.to_datetime(df_sleep['endtime'])

                # Save to Silver
                # Assuming the bronze data is already one row per sleep session
                save_idempotent(df_sleep, 'sleep_log', conn)
                
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM SILVER.SLEEP_LOG WHERE load_id = {load_id}")
                row_count = cursor.fetchone()[0]
                cursor.close()
                log_transformation_end(conn, trans_id, 'SUCCESS', row_count)

            except Exception as e:
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping fitbit sleep: {e}")