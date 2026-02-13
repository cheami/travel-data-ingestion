import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_config():
    """Loads the dataset configuration from JSON."""
    config_path = "/opt/airflow/configs/datasets.json"
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            return config.get("datasets", {})
    except FileNotFoundError:
        print(f"Error: {config_path} not found.")
        return {}

def save_idempotent(df, table_name, hook, engine):
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
        hook.run(f"DELETE FROM silver.{table_name} WHERE load_id IN ({ids_str})")
    
    # Append data
    df.to_sql(table_name, engine, schema='silver', if_exists='append', index=False)
    print(f"Success: Wrote {len(df)} rows to silver.{table_name}")