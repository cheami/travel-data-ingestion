from __future__ import annotations

def transform_silver(**kwargs):
    """
    Reads raw data from Bronze, performs aggregations, and writes to Silver.
    """
    print("--- Starting Silver Transformation ---", flush=True)
    
    # Lazy load imports to prevent top-level code execution/memory usage during DAG parsing
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from transformations.utils import load_config
    from transformations.transactions import process_transactions
    from transformations.manual_logs import process_manual_logs
    from transformations.flight_logs import process_flight_logs
    from transformations.fitbit_steps import process_fitbit_steps
    from transformations.fitbit_sleep import process_fitbit_sleep
    from transformations.fitbit_heart_rate import process_fitbit_heart_rate

    datasets_config = load_config()

    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    # Get optional parameters from DAG run config (JSON) OR Airflow Params (UI Form)
    conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    params = kwargs.get('params') or {}

    # Priority: Config JSON > UI Params
    target_transform = conf.get('transformation') or params.get('transformation')
    target_load_id = conf.get('job_id') or params.get('job_id')

    print(f"DEBUG: Configuration detected - Transformation: {target_transform}, Job ID: {target_load_id}", flush=True)

    # Execute transformations
    if not target_transform or target_transform == 'transactions':
        process_transactions(datasets_config, engine, hook, target_load_id)
    if not target_transform or target_transform == 'manual_logs':
        process_manual_logs(datasets_config, engine, hook, target_load_id)
    if not target_transform or target_transform == 'flight_logs':
        process_flight_logs(datasets_config, engine, hook, target_load_id)
    if not target_transform or target_transform == 'fitbit_steps':
        process_fitbit_steps(datasets_config, engine, hook, target_load_id)
    if not target_transform or target_transform == 'fitbit_sleep_score':
        process_fitbit_sleep(datasets_config, engine, hook, target_load_id)
    if not target_transform or target_transform == 'fitbit_heart_rate':
        process_fitbit_heart_rate(datasets_config, engine, hook, target_load_id)