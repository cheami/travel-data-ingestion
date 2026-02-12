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
            hook.run(f"DELETE FROM silver.{table_name} WHERE load_id IN ({ids_str})")
        
        # Append data
        df.to_sql(table_name, engine, schema='silver', if_exists='append', index=False)
        print(f"Success: Wrote {len(df)} rows to silver.{table_name}")

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
            
            # Save exact copy to Silver
            save_idempotent(df_trans, 'all_spending')

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

    # --- 3. Process Flight Logs ---
    print("Processing Flight Logs...")
    flight_config = datasets_config.get('flight_logs', {})
    flight_table = flight_config.get('target_table', 'flight_logs')

    try:
        df_flights = pd.read_sql(f"SELECT * FROM bronze.{flight_table}", engine)
        if not df_flights.empty:
            # Normalize columns: lower case and replace spaces with underscores
            df_flights.columns = df_flights.columns.str.strip().str.lower().str.replace(' ', '_')

            # Regex pattern to extract City, Airport, IATA, ICAO
            # Format: "City / Airport Name (IATA/ICAO)"
            pattern = r'^(?P<city>.*?) / (?P<airport>.*?) \((?P<iata>.*?)/(?P<icao>.*?)\)$'

            # Apply extraction for 'from' column
            if 'from' in df_flights.columns:
                dep_data = df_flights['from'].str.extract(pattern)
                df_flights['departure_city'] = dep_data['city']
                df_flights['departure_airport'] = dep_data['airport']
                df_flights['departure_airport_code_iata'] = dep_data['iata']
                df_flights['departure_airport_code_icao'] = dep_data['icao']

            # Apply extraction for 'to' column
            if 'to' in df_flights.columns:
                arr_data = df_flights['to'].str.extract(pattern)
                df_flights['arrival_city'] = arr_data['city']
                df_flights['arrival_airport'] = arr_data['airport']
                df_flights['arrival_airport_code_iata'] = arr_data['iata']
                df_flights['arrival_airport_code_icao'] = arr_data['icao']

            # Apply extraction for 'airline' column
            if 'airline' in df_flights.columns:
                airline_pattern = r'^(?P<name>.*?) \((?P<iata>.*?)/(?P<icao>.*?)\)$'
                airline_data = df_flights['airline'].str.extract(airline_pattern)
                df_flights['airline_name'] = airline_data['name']
                df_flights['airline_iata'] = airline_data['iata']
                df_flights['airline_icao'] = airline_data['icao']

            # Drop original composite columns
            df_flights.drop(columns=['from', 'to', 'airline'], inplace=True, errors='ignore')

            save_idempotent(df_flights, 'flight_logs')

    except Exception as e:
        print(f"Skipping flight logs: {e}")

    # --- 4. Process Fitbit Steps ---
    print("Processing Fitbit Steps...")
    steps_config = datasets_config.get('fitbit_steps', {})
    steps_table = steps_config.get('target_table', 'fitbit_steps')

    try:
        df_steps = pd.read_sql(f"SELECT * FROM bronze.{steps_table}", engine)
        if not df_steps.empty:
            # Convert timestamp to datetime
            df_steps['timestamp'] = pd.to_datetime(df_steps['timestamp'])
            df_steps['date'] = df_steps['timestamp'].dt.date
            df_steps['hour'] = df_steps['timestamp'].dt.hour

            # Aggregate steps by date and hour
            # We take the max load_id for the group to maintain lineage
            hourly_agg = df_steps.groupby(['date', 'hour']).agg({'steps': 'sum', 'load_id': 'max'}).reset_index()

            # Ensure 24 rows for every date (0-23 hours)
            dates = hourly_agg['date'].unique()
            all_combinations = [{'date': d, 'hour': h} for d in dates for h in range(24)]
            df_full = pd.DataFrame(all_combinations)

            # Merge aggregated data with the full 24-hour frame
            df_final = pd.merge(df_full, hourly_agg, on=['date', 'hour'], how='left')

            # Fill missing steps with 0
            df_final['steps'] = df_final['steps'].fillna(0).astype(int)

            # Fill missing load_id with the max load_id for that date (so they are grouped correctly)
            date_load_map = df_steps.groupby('date')['load_id'].max().to_dict()
            df_final['load_id'] = df_final['load_id'].fillna(df_final['date'].map(date_load_map)).astype(int)

            save_idempotent(df_final, 'hourly_step_count')

    except Exception as e:
        print(f"Skipping fitbit steps: {e}")