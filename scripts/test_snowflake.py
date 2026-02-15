import json
import os
import snowflake.connector

# Path to the variables file
CONFIG_PATH = '/home/cheami-linux/travel-data-ingestion/configs/airflow_variables.json'

def test_connection():
    # 1. Load configurations
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: Config file not found at {CONFIG_PATH}")
        return

    with open(CONFIG_PATH, 'r') as f:
        configs = json.load(f)

    print(f"Loaded config from {CONFIG_PATH}")

    # 2. Map JSON keys to Snowflake connection parameters
    try:
        conn_params = {
            'user': configs['snowflake_user'],
            'password': configs['snowflake_password'],
            'account': configs['snowflake_account'],
            'warehouse': configs['snowflake_warehouse'],
            'database': configs['snowflake_database'],
            'schema': configs['snowflake_schema'],
            'role': configs['snowflake_role']
        }
    except KeyError as e:
        print(f"Error: Missing expected key in config file: {e}")
        return

    # 3. Attempt Connection
    print(f"Connecting to Snowflake account: {conn_params['account']}...")
    try:
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cs:
                cs.execute("SELECT current_version(), current_role(), current_warehouse()")
                row = cs.fetchone()
                print(f"\nSUCCESS! Connected to Snowflake.\nVersion: {row[0]}\nRole: {row[1]}\nWarehouse: {row[2]}")
    except Exception as e:
        print(f"\nFAILURE: Could not connect to Snowflake.\nError: {e}")

if __name__ == "__main__":
    test_connection()
