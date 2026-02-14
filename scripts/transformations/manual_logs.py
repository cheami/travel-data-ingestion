import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end

def process_manual_logs(datasets_config, engine, hook, load_id=None):
    print("Processing Manual Logs...")
    logs_config = datasets_config.get('manual_logs', {})
    logs_table = logs_config.get('target_table', 'manual_logs')

    try:
        if load_id:
            load_ids = [load_id]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{logs_table}", engine)
            load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            trans_id = log_transformation_start(hook, load_id, 'manual_logs', 'manual_logs')
            try:
                df_logs = pd.read_sql(f"SELECT * FROM bronze.{logs_table} WHERE load_id = {load_id}", engine)
                if not df_logs.empty:
                    # Pass through exactly as is (preserving row_id from Bronze)
                    # We still normalize columns for consistency in Silver
                    df_logs.columns = df_logs.columns.str.strip().str.lower()
                    
                    # Write to Silver (Idempotent)
                    save_idempotent(df_logs, "manual_logs", hook, engine)
                    log_transformation_end(hook, trans_id, 'SUCCESS', len(df_logs))
                else:
                    log_transformation_end(hook, trans_id, 'SUCCESS', 0)
            except Exception as e:
                log_transformation_end(hook, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping manual logs: {e}")