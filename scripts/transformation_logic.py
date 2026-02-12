from __future__ import annotations

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

def transform_silver():
    """
    Reads raw data from Bronze, performs aggregations, and writes to Silver.
    """
    print("--- Starting Silver Transformation ---")
    
    # Function to load dataset configurations
    def load_config():
        """Loads the dataset configuration from JSON."""
        config_path = "/opt/airflow/configs/datasets.json"
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
                return config.get("datasets", {})  # Adjust based on your actual JSON structure
        except FileNotFoundError:
            print(f"Error: {config_path} not found.")
            return {}

    datasets_config = load_config()

   # Connect to Postgres
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    daily_spend = pd.DataFrame()

    def save_idempotent(df, table_name):
        """Deletes existing load_ids before appending new data."""
        if df.empty or 'load_id' not in df.columns:
            return

        load_ids = df['load_id'].unique().tolist()
        if not load_ids or df.columns.empty:
            return

        # Check if table exists
        if hook.get_first(f"SELECT to_regclass('silver.{table_name}')")[0]:
            ids_str = ','.join(map(str, load_ids))
            print(f"Clearing existing data for load_ids: {load_ids} in {table_name}")
            hook.run(f"DELETE FROM travel.{table_name} WHERE load_id IN ({ids_str})")
        
        # Append data
        df.to_sql(table_name, engine, schema='silver', if_exists='append', index=False)
        print(f"Success: Wrote {len(df)} rows to travel.{table_name}")

    # --- 1. Process Transactions (Daily Spend) ---
    print("Processing Transactions...")
    trans_config = datasets_config.get('transactions', {})
    trans_table = trans_config.get('target_table', 'transactions')
    
    try:
        df_trans = pd.read_sql(f"SELECT * FROM bronze.{trans_table}", engine)
        if not df_trans.empty:
            df_trans.columns = df_trans.columns.str.strip().str.lower()

            # Ensure 'type' column exists (default if missing)
            if 'type' not in df_trans.columns:
                df_trans['type'] = 'uncategorized'

            # Convert amount to currency (numeric)
            df_trans['amount'] = df_trans['amount'].replace({r'[$,]': ''}, regex=True)
            df_trans['amount'] = pd.to_numeric(df_trans['amount'])

            # Aggregate: Daily Spend by Date, Type, AND load_id
            daily_spend = df_trans.groupby(['date', 'type', 'load_id'])['amount'].sum().reset_index()

            save_idempotent(daily_spend, 'daily_spend')

    except Exception as e:
        print(f"Skipping transactions: {e}")

    # --- 2. Process Manual Logs (Direct Move) ---
    print("Processing Manual Logs...")
    logs_config = datasets_config.get('manual_logs', {})
    logs_table = logs_config.get('target_table', 'manual_logs')

    try:
        df_logs = pd.read_sql(f"SELECT * FROM bronze.{logs_table}", engine)
        if not df_logs.empty:
            # Pass through exactly as is (preserving row_id from Bronze)
            # We still normalize columns for consistency in Silver
            df_logs.columns = df_logs.columns.str.strip().str.lower()
            
            # Write to Silver (Idempotent)
            save_idempotent(df_logs, "manual_logs")

    except Exception as e:
        print(f"Skipping manual logs: {e}")