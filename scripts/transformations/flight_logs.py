import pandas as pd
from transformations.utils import save_idempotent

def process_flight_logs(datasets_config, engine, hook, load_id=None):
    print("Processing Flight Logs...")
    flight_config = datasets_config.get('flight_logs', {})
    flight_table = flight_config.get('target_table', 'flight_logs')

    try:
        if load_id:
            load_ids = [load_id]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{flight_table}", engine)
            load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            df_flights = pd.read_sql(f"SELECT * FROM bronze.{flight_table} WHERE load_id = {load_id}", engine)
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

                save_idempotent(df_flights, 'flight_logs', hook, engine)

    except Exception as e:
        print(f"Skipping flight logs: {e}")