from __future__ import annotations
import snowflake.connector
from airflow.models import Variable
from ingestion_logic import load_config
from transformations.transactions import process_transactions
from transformations.manual_logs import process_manual_logs
from transformations.flight_logs import process_flight_logs
from transformations.fitbit_steps import process_fitbit_steps
from transformations.fitbit_sleep import process_fitbit_sleep
from transformations.fitbit_heart_rate import process_fitbit_heart_rate
from transformations.google_timeline import process_google_timeline

def transform_silver(**kwargs):
    """
    Reads raw data from Bronze, performs aggregations, and writes to Silver.
    """
    print("--- Starting Silver Transformation ---", flush=True)
 
    datasets_config = load_config()

    # Connect to Snowflake using Airflow Variables
    conn = snowflake.connector.connect(
        user=Variable.get("snowflake_user"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse=Variable.get("snowflake_warehouse"),
        database=Variable.get("snowflake_database"),
        schema=Variable.get("snowflake_schema"),
        role=Variable.get("snowflake_role")
    )
    
    # Get optional parameters from DAG run config (JSON) OR Airflow Params (UI Form)
    conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    params = kwargs.get('params') or {}

    # Priority: Config JSON > UI Params
    target_transform = conf.get('transformation') or params.get('transformation')
    target_load_id = conf.get('job_id') or params.get('job_id')
    target_reprocess = conf.get('Reprocess') or params.get('Reprocess')

    # Normalize Reprocess to boolean (default False/0)
    reprocess = str(target_reprocess).lower() in ['true', '1', 'yes', 'on'] if target_reprocess is not None else False

    print(f"DEBUG: Configuration detected - Transformation: {target_transform}, Job ID: {target_load_id}, Reprocess: {reprocess}", flush=True)

    # Execute transformations
    if not target_transform or target_transform == 'transactions':
        process_transactions(datasets_config, conn, target_load_id, reprocess=reprocess)
    if not target_transform or target_transform == 'manual_logs':
        process_manual_logs(datasets_config, conn, target_load_id, reprocess=reprocess)
    if not target_transform or target_transform == 'flight_logs':
        process_flight_logs(datasets_config, conn, target_load_id, reprocess=reprocess)
    if not target_transform or target_transform == 'fitbit_steps':
        process_fitbit_steps(datasets_config, conn, target_load_id, reprocess=reprocess)
    if not target_transform or target_transform == 'fitbit_sleep_score':
        process_fitbit_sleep(datasets_config, conn, target_load_id, reprocess=reprocess)
    if not target_transform or target_transform == 'fitbit_heart_rate':
        process_fitbit_heart_rate(datasets_config, conn, target_load_id, reprocess=reprocess)
    if not target_transform or target_transform == 'google_timeline':
        process_google_timeline(datasets_config, conn, target_load_id, reprocess=reprocess)