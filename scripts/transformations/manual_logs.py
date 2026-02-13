import pandas as pd
from transformations.utils import save_idempotent

def process_manual_logs(datasets_config, engine, hook):
    print("Processing Manual Logs...")
    logs_config = datasets_config.get('manual_logs', {})
    logs_table = logs_config.get('target_table', 'manual_logs')

    try:
        load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{logs_table}", engine)
        load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            df_logs = pd.read_sql(f"SELECT * FROM bronze.{logs_table} WHERE load_id = {load_id}", engine)
            if not df_logs.empty:
                # Pass through exactly as is (preserving row_id from Bronze)
                # We still normalize columns for consistency in Silver
                df_logs.columns = df_logs.columns.str.strip().str.lower()
                
                # Write to Silver (Idempotent)
                save_idempotent(df_logs, "manual_logs", hook, engine)

    except Exception as e:
        print(f"Skipping manual logs: {e}")