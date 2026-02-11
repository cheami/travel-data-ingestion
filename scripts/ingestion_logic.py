from __future__ import annotations

import json
import os
import pendulum
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Path to the configuration file inside the container
CONFIG_PATH = "/opt/airflow/configs/datasets.json"
DATA_PATH = "/opt/airflow/data/landing"

# Function to retrieve dataset configurations
def load_config():
    """Loads the dataset configuration from JSON."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# Main logic for processing files and loading them into the database
def ingest_dataset(dataset_name, config, **kwargs):
    """
    Simulates the ingestion logic.
    In the future, this will connect to Azure Blob and Snowflake.
    For now, it scans the local 'landing' folder.
    """
    source_subfolder = config.get("source_path")
    file_pattern = config.get("file_pattern")
    
    full_path = os.path.join(DATA_PATH, source_subfolder)
    
    print(f"--- Starting Ingestion for {dataset_name} ---")
    print(f"Looking for files in: {full_path}")
    print(f"Matching pattern: {file_pattern}")
    
    # Simulation of checking for files
    if os.path.exists(full_path):
        files = [f for f in os.listdir(full_path) if f.endswith(file_pattern.replace("*", ""))]
        if files:
            print(f"Found {len(files)} files to process: {files}")
            
            # Connect to Postgres (Bronze Layer)
            hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # Ensure Schema Exists
            hook.run("CREATE SCHEMA IF NOT EXISTS travel;")
            
            # 1. Ensure Logging Table Exists
            hook.run("""
                CREATE TABLE IF NOT EXISTS travel.ingestion_logs (
                    load_id SERIAL PRIMARY KEY,
                    dataset_name VARCHAR(50),
                    file_name VARCHAR(255),
                    file_size_bytes BIGINT,
                    file_type VARCHAR(20),
                    row_count INT,
                    status VARCHAR(20),
                    error_message TEXT,
                    ingestion_timestamp TIMESTAMP
                );
            """)
            
            engine = hook.get_sqlalchemy_engine()

            for filename in files:
                file_path = os.path.join(full_path, filename)
                
                # 2. Idempotency Check: Has this file been loaded successfully?
                check_sql = "SELECT 1 FROM travel.ingestion_logs WHERE file_name = %s AND status = 'SUCCESS'"
                if hook.get_first(check_sql, parameters=(filename,)):
                    print(f"Skipping {filename}: Already loaded successfully.")
                    continue

                print(f"Processing {filename}...")
                file_size = os.path.getsize(file_path)
                
                # 3. Start Log (Get load_id)
                insert_init_sql = """
                    INSERT INTO travel.ingestion_logs 
                    (dataset_name, file_name, file_size_bytes, file_type, status, ingestion_timestamp)
                    VALUES (%s, %s, %s, %s, 'RUNNING', %s)
                    RETURNING load_id;
                """
                load_id = hook.get_first(insert_init_sql, parameters=(
                    dataset_name, 
                    filename, 
                    file_size, 
                    config['format'], 
                    pendulum.now()
                ))[0]

                row_count = 0
                status = "SUCCESS"
                error_message = None

                try:
                    # Read Data based on format
                    if config['format'] == 'csv':
                        df = pd.read_csv(file_path)
                    elif config['format'] == 'json':
                        df = pd.read_json(file_path)
                    else:
                        raise ValueError(f"Unsupported format: {config['format']}")
                    
                    row_count = len(df)
                    
                    # Add Metadata
                    df['_ingestion_time'] = pendulum.now()
                    df['_source_file'] = filename
                    df['load_id'] = load_id
                    df['row_id'] = [f"{load_id}_{i}" for i in range(len(df))]

                    # Ensure target table has load_id and row_id columns if it exists
                    check_table_sql = f"SELECT to_regclass('travel.{config['target_table']}')"
                    if hook.get_first(check_table_sql)[0]:
                        hook.run(f"ALTER TABLE travel.{config['target_table']} ADD COLUMN IF NOT EXISTS load_id INTEGER")
                        hook.run(f"ALTER TABLE travel.{config['target_table']} ADD COLUMN IF NOT EXISTS row_id VARCHAR(255)")

                    # Write to Postgres
                    df.to_sql(config['target_table'], engine, schema='travel', if_exists='append', index=False)
                    print(f"Loaded {row_count} rows into {config['target_table']}")

                except Exception as e:
                    status = "FAILURE"
                    error_message = str(e)
                    print(f"Error loading {filename}: {e}")
                
                # 4. Update Log (Success or Failure)
                update_log_sql = """
                    UPDATE travel.ingestion_logs 
                    SET row_count = %s, status = %s, error_message = %s
                    WHERE load_id = %s
                """
                hook.run(update_log_sql, parameters=(
                    row_count, 
                    status, 
                    error_message, 
                    load_id
                ))

        else:
            print("No new files found.")
    else:
        print(f"Directory {full_path} does not exist. Please create it and add data.")

    print(f"Target Table: {config.get('target_table')}")
    print("--- Ingestion Complete ---")