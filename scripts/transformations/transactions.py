import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end

def process_transactions(datasets_config, conn, load_id=None, reprocess=False):
    print("Processing Transactions...")
    trans_config = datasets_config.get('transactions', {})
    trans_table = trans_config.get('target_table', 'transactions')
    
    try:
        if load_id:
            load_ids = [int(load_id)]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{trans_table}", conn)
            load_ids_df.columns = [c.lower() for c in load_ids_df.columns]
            load_ids = load_ids_df['load_id'].tolist()

        if not reprocess:
            processed_df = pd.read_sql(f"SELECT DISTINCT load_id FROM ADMIN.TRANSFORMATION_LOGS WHERE TRANSFORMATION_NAME = 'transactions' AND status = 'SUCCESS'", conn)
            processed_df.columns = [c.lower() for c in processed_df.columns]
            processed_ids = set(processed_df['load_id'].tolist())
            load_ids = [lid for lid in load_ids if lid not in processed_ids]
            if not load_ids:
                print("No new load_ids to process for transactions.")

        for load_id in load_ids:
            trans_id = log_transformation_start(conn, load_id, 'transactions', 'daily_spend, all_spending')
            try:
                df_trans = pd.read_sql(f"SELECT * FROM bronze.{trans_table} WHERE load_id = {load_id}", conn)
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

                    save_idempotent(daily_spend, 'daily_spend', conn)
                    
                    # Save exact copy to Silver
                    save_idempotent(df_trans, 'all_spending', conn)
                    
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM SILVER.ALL_SPENDING WHERE load_id = {load_id}")
                    row_count = cursor.fetchone()[0]
                    cursor.close()
                    log_transformation_end(conn, trans_id, 'SUCCESS', row_count)
                else:
                    log_transformation_end(conn, trans_id, 'SUCCESS', 0)
            except Exception as e:
                log_transformation_end(conn, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping transactions: {e}")