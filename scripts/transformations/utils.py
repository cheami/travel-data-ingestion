import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

def map_pandas_dtype_to_snowflake(dtype):
    if pd.api.types.is_integer_dtype(dtype): return "NUMBER"
    if pd.api.types.is_float_dtype(dtype): return "FLOAT"
    if pd.api.types.is_bool_dtype(dtype): return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype): return "TIMESTAMP_TZ"
    return "STRING"

def save_idempotent(df, table_name, conn, schema='SILVER'):
    """
    Writes a DataFrame to Snowflake idempotently.
    Deletes existing rows for the load_ids present in the DataFrame before writing.
    """
    if df.empty:
        return

    cursor = conn.cursor()
    try:
        # Ensure schema exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cursor.execute(f"USE SCHEMA {schema}")

        # Uppercase table name and columns for Snowflake consistency
        table_name = table_name.upper()
        df.columns = [c.upper() for c in df.columns]

        # Create table if not exists
        col_defs = []
        for col, dtype in df.dtypes.items():
            col_defs.append(f'"{col}" {map_pandas_dtype_to_snowflake(dtype)}')
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        cursor.execute(create_sql)

        # Idempotency: Delete existing data for the load_ids present in the dataframe
        if 'LOAD_ID' in df.columns:
            load_ids = df['LOAD_ID'].unique()
            if len(load_ids) > 0:
                load_ids_str = ', '.join(map(str, load_ids))
                cursor.execute(f"DELETE FROM {table_name} WHERE LOAD_ID IN ({load_ids_str})")
        
        # Write data
        write_pandas(conn, df, table_name, schema=schema)
        
    finally:
        cursor.close()

def check_data_exists(conn, load_id, schema, table):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT 1 FROM {schema}.{table} WHERE load_id = %s LIMIT 1", (load_id,))
        return cursor.fetchone() is not None
    finally:
        cursor.close()

def log_transformation_start(conn, load_id, dataset_name, target_tables):
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS ADMIN")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ADMIN.TRANSFORMATION_LOGS (
                transformation_id NUMBER IDENTITY(1,1) PRIMARY KEY,
                load_id NUMBER,
                DATASET_NAME VARCHAR(100),
                target_table VARCHAR(100),
                status VARCHAR(20),
                rows_processed NUMBER,
                error_message TEXT,
                start_time TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                end_time TIMESTAMP_TZ
            )
        """)
        
        cursor.execute("""
            INSERT INTO ADMIN.TRANSFORMATION_LOGS (load_id, DATASET_NAME, target_table, status)
            VALUES (%s, %s, %s, 'RUNNING')
        """, (load_id, dataset_name, target_tables))
        
        # Get the generated ID
        cursor.execute("SELECT MAX(transformation_id) FROM ADMIN.TRANSFORMATION_LOGS WHERE load_id = %s AND DATASET_NAME = %s", (load_id, dataset_name))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()

def log_transformation_end(conn, trans_id, status, row_count, error_message=None):
    if not trans_id:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE ADMIN.TRANSFORMATION_LOGS
            SET status = %s, rows_processed = %s, error_message = %s, end_time = CURRENT_TIMESTAMP()
            WHERE transformation_id = %s
        """, (status, row_count, error_message, trans_id))
    finally:
        cursor.close()