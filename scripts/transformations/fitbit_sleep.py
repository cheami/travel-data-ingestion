import pandas as pd
from transformations.utils import save_idempotent, log_transformation_start, log_transformation_end

def process_fitbit_sleep(datasets_config, engine, hook, load_id=None):
    print("Processing Fitbit Sleep Score...")
    sleep_config = datasets_config.get('fitbit_sleep_score', {})
    sleep_table = sleep_config.get('target_table', 'fitbit_sleep_score')

    try:
        if load_id:
            load_ids = [load_id]
        else:
            load_ids_df = pd.read_sql(f"SELECT DISTINCT load_id FROM bronze.{sleep_table}", engine)
            load_ids = load_ids_df['load_id'].tolist()

        for load_id in load_ids:
            trans_id = log_transformation_start(hook, load_id, 'fitbit_sleep_score', 'sleep_log, sleep_daily_summary')
            try:
                df_sleep = pd.read_sql(f"SELECT * FROM bronze.{sleep_table} WHERE load_id = {load_id}", engine)
                if not df_sleep.empty:
                    # --- 1. Create Enriched Table (sleep_log) ---
                    df_sleep['sleep_start_time'] = pd.to_datetime(df_sleep['timestamp'])
                    df_sleep['sleep_date'] = df_sleep['sleep_start_time'].dt.date
                    df_sleep['day_of_week'] = df_sleep['sleep_start_time'].dt.day_name()
                    # is_weekend: 5=Saturday, 6=Sunday
                    df_sleep['is_weekend'] = df_sleep['sleep_start_time'].dt.dayofweek.isin([5, 6])
                    
                    df_sleep['restlessness'] = df_sleep['restlessness'].round(4)
                    
                    def get_quality(score):
                        if score >= 90: return 'Excellent'
                        if score >= 80: return 'Good'
                        if score >= 60: return 'Fair'
                        return 'Poor'
                    
                    df_sleep['sleep_quality_category'] = df_sleep['overall_score'].apply(get_quality)
                    df_sleep['has_detailed_breakdown'] = df_sleep['composition_score'].notnull()

                    # Select columns to match DDL and drop original timestamp if needed
                    # We keep metadata columns (load_id, row_id, etc.)
                    cols_to_keep = [
                        'sleep_log_entry_id', 'sleep_start_time', 'sleep_date', 'day_of_week', 'is_weekend',
                        'overall_score', 'composition_score', 'revitalization_score', 'duration_score',
                        'deep_sleep_in_minutes', 'resting_heart_rate', 'restlessness',
                        'sleep_quality_category', 'has_detailed_breakdown',
                        'load_id', 'row_id', '_ingestion_time', '_source_file'
                    ]
                    sleep_log_df = df_sleep[[c for c in cols_to_keep if c in df_sleep.columns]].copy()
                    save_idempotent(sleep_log_df, 'sleep_log', hook, engine)

                    # --- 2. Create Aggregated Table (sleep_daily_summary) ---
                    # Group by sleep_date AND load_id to maintain lineage for idempotency
                    daily_agg = df_sleep.groupby(['sleep_date', 'load_id']).agg(
                        avg_overall_score=('overall_score', 'mean'),
                        avg_resting_heart_rate=('resting_heart_rate', 'mean'),
                        total_deep_sleep_minutes=('deep_sleep_in_minutes', 'sum'),
                        sleep_session_count=('sleep_log_entry_id', 'count')
                    ).reset_index()

                    daily_agg['avg_overall_score'] = daily_agg['avg_overall_score'].round(1)
                    daily_agg['avg_resting_heart_rate'] = daily_agg['avg_resting_heart_rate'].round(1)

                    save_idempotent(daily_agg, 'sleep_daily_summary', hook, engine)
                    log_transformation_end(hook, trans_id, 'SUCCESS', len(df_sleep))
                else:
                    log_transformation_end(hook, trans_id, 'SUCCESS', 0)
            except Exception as e:
                log_transformation_end(hook, trans_id, 'FAILURE', 0, str(e))
                raise e

    except Exception as e:
        print(f"Skipping fitbit sleep score: {e}")