from __future__ import annotations

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import modularized functions
from transformations.utils import load_config
from transformations.transactions import process_transactions
from transformations.manual_logs import process_manual_logs
from transformations.flight_logs import process_flight_logs
from transformations.fitbit_steps import process_fitbit_steps
from transformations.fitbit_sleep import process_fitbit_sleep
from transformations.fitbit_heart_rate import process_fitbit_heart_rate

def transform_silver():
    """
    Reads raw data from Bronze, performs aggregations, and writes to Silver.
    """
    print("--- Starting Silver Transformation ---")
    
    datasets_config = load_config()

    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    # Execute transformations
    process_transactions(datasets_config, engine, hook)
    process_manual_logs(datasets_config, engine, hook)
    process_flight_logs(datasets_config, engine, hook)
    process_fitbit_steps(datasets_config, engine, hook)
    process_fitbit_sleep(datasets_config, engine, hook)
    process_fitbit_heart_rate(datasets_config, engine, hook)