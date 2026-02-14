import json
import pandas as pd
import pendulum
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

def ensure_log_table_exists(hook):
    """Ensures the transformation log table exists."""
    hook.run("""
        CREATE TABLE IF NOT EXISTS admin.transformation_logs (
            transformation_id SERIAL PRIMARY KEY,
            load_id INTEGER,
            transformation_name VARCHAR(100),
            target_table VARCHAR(100),
            status VARCHAR(20),
            rows_processed INTEGER,
            error_message TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP
        );
    """)

def log_transformation_start(hook, load_id, transformation_name, target_table):
    """Logs the start of a transformation."""
    ensure_log_table_exists(hook)
    sql = """
        INSERT INTO admin.transformation_logs 
        (load_id, transformation_name, target_table, status, start_time)
        VALUES (%s, %s, %s, 'RUNNING', %s)
        RETURNING transformation_id;
    """
    return hook.get_first(sql, parameters=(load_id, transformation_name, target_table, pendulum.now()))[0]

def log_transformation_end(hook, transformation_id, status, rows_processed, error_message=None):
    """Logs the end of a transformation."""
    sql = """
        UPDATE admin.transformation_logs
        SET status = %s, rows_processed = %s, error_message = %s, end_time = %s
        WHERE transformation_id = %s;
    """
    hook.run(sql, parameters=(status, rows_processed, error_message, pendulum.now(), transformation_id))