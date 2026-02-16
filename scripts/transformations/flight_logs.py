import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end, check_data_exists

def process_flight_logs(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Flight Logs...")
    flights_config = datasets_config.get('flight_logs', {})
    flights_table = flights_config.get('target_table', 'flight_logs')

    try:
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{flights_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE DATASET_NAME = 'flight_logs' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for flight_logs.")

        for load_id in load_ids:
            if not check_data_exists(conn, load_id, 'bronze', flights_table):
                print(f"Skipping load_id {load_id} for flight_logs (no data in bronze).")
                continue

            trans_id = log_transformation_start(conn, load_id, 'flight_logs', 'flight_logs')
            try:
                df_flights = pd.read_sql(f"SELECT * FROM bronze.{flights_table} WHERE load_id = {load_id}", conn)
                if not df_flights.empty:
                    # Normalize columns
                    df_flights.columns = df_flights.columns.str.strip().str.lower()
                    
                    # Basic cleaning
                    if 'date' in df_flights.columns:
                        df_flights['date'] = pd.to_datetime(df_flights['date']).dt.date
                    
                    # Write to Silver
                    save_idempotent(df_flights, 'flight_logs', conn)
                    
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM SILVER.FLIGHT_LOGS WHERE load_id = {load_id}")
                    row_count = cursor.fetchone()[0]
                    cursor.close()
                    log_transformation_end(conn, trans_id, 'SUCCESS', row_count)
                else:
                    log_transformation_end(conn, trans_id, 'SUCCESS', 0)
            except Exception as e:
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping flight logs: {e}")