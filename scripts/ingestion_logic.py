from __future__ import annotations

import os
import snowflake.connector
from airflow.models import Variable

# Function to retrieve dataset configurations
def load_config():
    """Loads the dataset configuration from Snowflake ADMIN.FILE_DETAILS."""
    conn = get_snowflake_conn()
    try:
        cs = conn.cursor()
        cs.execute("""
            SELECT file_id, container, stage, source_path, file_pattern, 
                   target_schema, target_table, format 
            FROM ADMIN.FILE_DETAILS
        """)
        rows = cs.fetchall()
        columns = [col[0].lower() for col in cs.description]
        
        configs = {}
        for row in rows:
            cfg = dict(zip(columns, row))
            # Use target_table (lowercased) as the key to match previous behavior
            configs[cfg['target_table'].lower()] = cfg
        return configs
    finally:
        conn.close()

def get_system_config():
    """Returns system configuration."""
    return {"ingestion_log_table": "ADMIN.INGESTION_LOGS"}

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

def get_table_columns(cursor, table_name):
    """Retrieves column names from a Snowflake table, excluding metadata columns."""
    cursor.execute(f"DESC TABLE {table_name}")
    rows = cursor.fetchall()
    # Filter out metadata columns
    excluded = {'ROW_ID'}
    return [row[0] for row in rows if row[0].upper() not in excluded]

# Main logic for processing files and loading them into the database
def ingest_dataset(dataset_name, config, **kwargs):
    """
    Ingests data from Azure Blob (via Snowflake Stage) to Snowflake tables.
    """
    stage = config.get("stage").upper()
    if stage and "." not in stage:
        stage = f"ADMIN.{stage}"
    source_path = config.get("source_path")
    file_pattern = config.get("file_pattern")
    file_fmt = config.get("format").upper()
    target_schema = config.get("target_schema", "bronze").strip().upper()
    target_table = config.get("target_table").upper()
    config_file_id = config.get("file_id")
    
    print(f"--- Starting Ingestion for {dataset_name} ---")
    print(f"Stage: {stage}, Path: {source_path}, Pattern: {file_pattern}")

    system_config = get_system_config()
    log_table = system_config.get("ingestion_log_table", "ADMIN.INGESTION_LOGS").upper()
    
    conn = get_snowflake_conn()
    cs = conn.cursor()

    try:
        db_name = Variable.get("snowflake_database").strip().upper()
        cs.execute(f"USE DATABASE {db_name}")
        cs.execute(f"USE SCHEMA {target_schema}")
        
        full_target_table = f"{db_name}.{target_schema}.{target_table}"
        
        # Get columns and prepare select statement
        columns = [f'"{col.replace(" ", "_").upper()}"' for col in get_table_columns(cs, full_target_table)]
        columns_str = ", ".join(columns)
        
        if file_fmt == 'JSON':
            select_str = "$1"
        else:
            select_str = ", ".join([f"${i+1}" for i in range(len(columns) - 3)])
        
        # Ensure Logging Table Exists with new file_id column
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                load_id NUMBER IDENTITY(1,1) PRIMARY KEY,
                file_id NUMBER,
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

        # List files in stage
        # Convert simple glob to regex for Snowflake LIST PATTERN if needed
        regex_pattern = file_pattern.replace('.', '\\.').replace('*', '.*')
        
        list_sql = f"LIST @{stage}/{source_path} PATTERN='{regex_pattern}'"
        print(f"Executing: {list_sql}")
        cs.execute(list_sql)
        files = cs.fetchall()
        
        if not files:
            print("No files found.")
            return

        print(f"Found {len(files)} files.")

        for row in files:
            # row[0] includes the stage name (e.g., "AZURE_STAGE/path/to/file.csv")
            # We need to strip the stage name to get the relative path
            full_file_path = row[0]
            if '/' in full_file_path:
                full_file_path = full_file_path.split('/', 1)[1]

            file_name = os.path.basename(full_file_path)
            file_size = row[1]
            
            # Idempotency Check
            check_sql = f"SELECT 1 FROM {log_table} WHERE file_name = %s AND status = 'SUCCESS'"
            cs.execute(check_sql, (file_name,))
            if cs.fetchone():
                print(f"Skipping {file_name}: Already loaded successfully.")
                continue

            print(f"Processing {file_name}...")
            
            # Start Log
            insert_init_sql = f"""
                INSERT INTO {log_table} 
                (file_id, dataset_name, file_name, file_size_bytes, file_type, status, ingestion_timestamp, target_schema, target_table)
                VALUES (%s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP(), %s, %s)
            """
            cs.execute(insert_init_sql, (
                config_file_id,
                dataset_name, 
                file_name, 
                file_size, 
                file_fmt, 
                target_schema,
                target_table
            ))
            
            cs.execute(f"SELECT MAX(load_id) FROM {log_table} WHERE file_name = %s", (file_name,))
            load_id = cs.fetchone()[0]

            row_count = 0
            status = "SUCCESS"
            error_message = None

            try:
                if file_fmt == 'JSON':
                    format_opts = f"FORMAT_NAME = '{file_fmt}'"
                else:
                    format_opts = f"FORMAT_NAME = '{file_fmt}', error_on_column_count_mismatch=false"

                # COPY INTO Temporary Table
                copy_sql = f"""
                    COPY INTO {full_target_table} ({columns_str})
                    FROM (
                        SELECT {select_str}, CURRENT_TIMESTAMP(), '{file_name}', {load_id}
                        FROM @{stage}/{source_path}{file_name}
                    )
                    FILE_FORMAT = ({format_opts})
                    ON_ERROR = 'SKIP_FILE'
                """
                cs.execute(copy_sql)
                
                # Get row count
                res = cs.fetchone()
                
                if res and len(res) >= 4:
                    # Result columns: file, status, rows_parsed, rows_loaded, ...
                    copy_status = res[1]
                    row_count = res[3]
                    
                    if copy_status == 'LOAD_FAILED':
                        raise Exception(f"Copy failed: {res[6]}")
                    
                    print(f"Loaded {row_count} rows into {full_target_table}")
                else:
                    print(f"Info: No result returned for {file_name} (possibly already loaded).")
                    cs.execute(f"SELECT COUNT(*) FROM {full_target_table} WHERE LOAD_ID = %s", (load_id,))
                    row_count = cs.fetchone()[0]

            except Exception as e:
                status = "FAILURE"
                error_message = str(e)
                print(f"Error loading {file_name}: {e}")
            
            update_log_sql = f"""
                UPDATE {log_table} 
                SET row_count = %s, status = %s, error_message = %s
                WHERE load_id = %s
            """
            cs.execute(update_log_sql, (row_count, status, error_message, load_id))
            
    except Exception as e:
        print(f"ERROR: An exception occurred during Snowflake operations: {e}")
        raise e
    finally:
        cs.close()
        conn.close()

    print(f"Target Table: {config.get('target_table')}")
    print("--- Ingestion Complete ---")