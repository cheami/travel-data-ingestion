import pandas as pd
from transformations.utils import log_transformation_start, log_transformation_end, check_data_exists

def process_google_timeline(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Google Timeline...")
    
    timeline_table = "google_timeline" 
    
    try:
        # get load ids
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{timeline_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        # filter processed
        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE DATASET_NAME = 'google_timeline' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for google_timeline.")

        for load_id in load_ids:
            load_id = int(load_id)
            if not check_data_exists(conn, load_id, 'bronze', timeline_table):
                print(f"Skipping load_id {load_id} for google_timeline (no data in bronze).")
                continue

            trans_id = log_transformation_start(conn, load_id, 'google_timeline', 'google_timeline')
            try:
                # call sp
                cursor = conn.cursor()
                try:
                    cursor.execute(f"CALL SILVER.PROCESS_GOOGLE_TIMELINE({load_id})")
                    
                    # get count
                    cursor.execute(f"SELECT COUNT(*) FROM SILVER.GOOGLE_TIMELINE WHERE load_id = {load_id}")
                    row_count = cursor.fetchone()[0]
                finally:
                    cursor.close()
                
                log_transformation_end(conn, trans_id, 'SUCCESS', row_count)

            except Exception as e:
                print(f"Error processing load_id {load_id}: {e}")
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Error processing Google Timeline: {e}")
        raise e