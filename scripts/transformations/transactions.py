import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end

def process_transactions(datasets_config, engine, hook, load_id=None):
    print("Processing Transactions...")
    trans_config = datasets_config.get('transactions', {})
    trans_table = trans_config.get('target_table', 'transactions')
    
    try:
        if load_id:
            load_ids = [load_id]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{trans_table}", engine)
            load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            trans_id = log_transformation_start(hook, load_id, 'transactions', 'daily_spend, all_spending')
            try:
                df_trans = pd.read_sql(f"SELECT * FROM bronze.{trans_table} WHERE load_id = {load_id}", engine)
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

                    save_idempotent(daily_spend, 'daily_spend', hook, engine)
                    
                    # Save exact copy to Silver
                    save_idempotent(df_trans, 'all_spending', hook, engine)
                    log_transformation_end(hook, trans_id, 'SUCCESS', len(df_trans))
                else:
                    log_transformation_end(hook, trans_id, 'SUCCESS', 0)
            except Exception as e:
                log_transformation_end(hook, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping transactions: {e}")