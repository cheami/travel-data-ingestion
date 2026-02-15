from __future__ import annotations

import json
import os
import pendulum
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable

# Path to the configuration file inside the container
CONFIG_PATH = "/opt/airflow/configs/datasets.json"
DATA_PATH = "/opt/airflow/data/landing"

# Function to retrieve dataset configurations
def load_config():
    """Loads the dataset configuration from JSON."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
        return config.get("datasets", config)

def get_system_config():
    """Loads system configuration from JSON."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
        return config.get("system_config", {})

def get_snowflake_conn():
    """Establishes a connection to Snowflake using Airflow Variables."""
    return snowflake.connector.connect(
        user=Variable.get("snowflake_user"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse=Variable.get("snowflake_warehouse"),
        database=Variable.get("snowflake_database"),
        schema=Variable.get("snowflake_schema"),
        role=Variable.get("snowflake_role")
    )

def map_pandas_dtype_to_snowflake(dtype):
    """Maps pandas dtypes to Snowflake SQL types."""
    if pd.api.types.is_integer_dtype(dtype): return "NUMBER"
    if pd.api.types.is_float_dtype(dtype): return "FLOAT"
    if pd.api.types.is_bool_dtype(dtype): return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype): return "TIMESTAMP_TZ"
    return "STRING"

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

    system_config = get_system_config()
    log_table = system_config.get("ingestion_log_table", "ADMIN.INGESTION_LOGS").upper()
    target_schema = config.get("target_schema", "bronze").strip().upper()
    
    # Simulation of checking for files
    if os.path.exists(full_path):
        files = [f for f in os.listdir(full_path) if f.endswith(file_pattern.replace("*", ""))]
        if files:
            print(f"Found {len(files)} files to process: {files}")
            
            # Connect to Snowflake
            conn = get_snowflake_conn()
            cs = conn.cursor()

            try:
                # Explicitly set the database context
                db_name = Variable.get("snowflake_database").strip().upper()
                cs.execute(f"USE DATABASE {db_name}")

                # Ensure Schema Exists
                cs.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
                cs.execute(f"USE SCHEMA {target_schema}")
                cs.execute(f"CREATE SCHEMA IF NOT EXISTS ADMIN")
                
                # 1. Ensure Logging Table Exists (Snowflake Syntax)
                cs.execute(f"""
                    CREATE TABLE IF NOT EXISTS {log_table} (
                        load_id NUMBER IDENTITY(1,1) PRIMARY KEY,
                        dataset_name VARCHAR(50),
                        file_name VARCHAR(255),
                        file_size_bytes NUMBER,
                        file_type VARCHAR(20),
                        row_count NUMBER,
                        status VARCHAR(20),
                        error_message STRING,
                        ingestion_timestamp TIMESTAMP_TZ,
                        target_schema VARCHAR(50),
                        target_table VARCHAR(100)
                    )
                """)

                for filename in files:
                    file_path = os.path.join(full_path, filename)
                    
                    # 2. Idempotency Check
                    check_sql = f"SELECT 1 FROM {log_table} WHERE file_name = %s AND status = 'SUCCESS'"
                    cs.execute(check_sql, (filename,))
                    if cs.fetchone():
                        print(f"Skipping {filename}: Already loaded successfully.")
                        continue

                    print(f"Processing {filename}...")
                    file_size = os.path.getsize(file_path)
                    
                    # 3. Start Log (Get load_id)
                    # Snowflake doesn't always support RETURNING in the same way for all connectors, 
                    # so we insert and then fetch the generated ID.
                    insert_init_sql = f"""
                        INSERT INTO {log_table} 
                        (dataset_name, file_name, file_size_bytes, file_type, status, ingestion_timestamp, target_schema, target_table)
                        VALUES (%s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP(), %s, %s)
                    """
                    cs.execute(insert_init_sql, (
                        dataset_name, 
                        filename, 
                        file_size, 
                        config['format'], 
                        target_schema,
                        config['target_table']
                    ))
                    
                    # Retrieve the generated load_id
                    cs.execute(f"SELECT MAX(load_id) FROM {log_table} WHERE file_name = %s", (filename,))
                    load_id = cs.fetchone()[0]

                    row_count = 0
                    status = "SUCCESS"
                    error_message = None

                    try:
                        # Read Data based on format
                        if config['format'] == 'csv':
                            df = pd.read_csv(file_path)
                        elif config['format'] == 'json':
                            try:
                                df = pd.read_json(file_path)
                            except ValueError:
                                with open(file_path, 'r') as f:
                                    raw_data = json.load(f)
                                df = pd.DataFrame([{'raw_data': raw_data}])
                        else:
                            raise ValueError(f"Unsupported format: {config['format']}")
                        
                        row_count = len(df)
                        
                        # Add Metadata
                        df['_INGESTION_TIME'] = pd.Timestamp.now(tz='UTC').isoformat()
                        df['_SOURCE_FILE'] = filename
                        df['LOAD_ID'] = load_id
                        df['ROW_ID'] = [f"{load_id}_{i}" for i in range(len(df))]

                        # Uppercase columns for Snowflake consistency
                        df.columns = [c.upper() for c in df.columns]
                        target_table_full = f"{db_name}.{target_schema}.{config['target_table']}".upper()

                        # Create Target Table if it doesn't exist (write_pandas requires table to exist)
                        # Construct DDL
                        col_defs = []
                        for col, dtype in df.dtypes.items():
                            if col == 'RAW_DATA':
                                col_defs.append(f'"{col}" VARIANT')
                            else:
                                col_defs.append(f'"{col}" {map_pandas_dtype_to_snowflake(dtype)}')
                        create_tbl_sql = f"CREATE TABLE IF NOT EXISTS {target_table_full} ({', '.join(col_defs)})"
                        cs.execute(create_tbl_sql)

                        # Write to Snowflake
                        success, nchunks, nrows, _ = write_pandas(
                            conn, 
                            df, 
                            config['target_table'].upper(), 
                            schema=target_schema.upper(),
                            database=db_name,
                            index=False
                        )
                        print(f"Loaded {nrows} rows into {target_table_full}")

                    except Exception as e:
                        status = "FAILURE"
                        error_message = str(e)
                        print(f"Error loading {filename}: {e}")
                    
                    # 4. Update Log (Success or Failure)
                    update_log_sql = f"""
                        UPDATE {log_table} 
                        SET row_count = %s, status = %s, error_message = %s
                        WHERE load_id = %s
                    """
                    cs.execute(update_log_sql, (
                        row_count, 
                        status, 
                        error_message, 
                        load_id
                    ))
            except Exception as e:
                print(f"ERROR: An exception occurred during Snowflake operations: {e}")
                raise e
            finally:
                cs.close()
                conn.close()

        else:
            print("No new files found.")
    else:
        print(f"Directory {full_path} does not exist. Please create it and add data.")

    print(f"Target Table: {config.get('target_table')}")
    print("--- Ingestion Complete ---")