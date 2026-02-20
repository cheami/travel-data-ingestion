import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end, check_data_exists

def process_manual_logs(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Manual Logs...")
    logs_config = datasets_config.get('manual_logs', {})
    logs_table = logs_config.get('target_table', 'manual_logs')

    try:
        # get load ids
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{logs_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        # filter processed
        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE DATASET_NAME = 'manual_logs' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for manual_logs.")

        for load_id in load_ids:
            if not check_data_exists(conn, load_id, 'bronze', logs_table):
                print(f"Skipping load_id {load_id} for manual_logs (no data in bronze).")
                continue

            trans_id = log_transformation_start(conn, load_id, 'manual_logs', 'manual_logs')
            try:
                # read data
                df_logs = pd.read_sql(f"SELECT * FROM bronze.{logs_table} WHERE load_id = {load_id}", conn)
                
                if df_logs.empty:
                    log_transformation_end(conn, trans_id, 'SUCCESS', 0)
                    continue

                df_logs.columns = df_logs.columns.str.strip().str.lower()
                
                # save
                save_idempotent(df_logs, "manual_logs", conn)
                
                # log success
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM SILVER.MANUAL_LOGS WHERE load_id = {load_id}")
                row_count = cursor.fetchone()[0]
                cursor.close()
                log_transformation_end(conn, trans_id, 'SUCCESS', row_count)

            except Exception as e:
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping manual logs: {e}")