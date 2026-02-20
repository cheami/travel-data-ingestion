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
    # start
    print("--- Starting Silver Transformation ---", flush=True)
 
    datasets_config = load_config()

    # snowflake conn
    conn = snowflake.connector.connect(
        user=Variable.get("snowflake_user"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse=Variable.get("snowflake_warehouse"),
        database=Variable.get("snowflake_database"),
        schema=Variable.get("snowflake_schema"),
        role=Variable.get("snowflake_role")
    )
    
    # get params
    conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    params = kwargs.get('params') or {}

    target_transform = conf.get('transformation') or params.get('transformation')
    target_load_id = conf.get('job_id') or params.get('job_id')
    target_reprocess = conf.get('Reprocess') or params.get('Reprocess')

    # fix bool
    reprocess = str(target_reprocess).lower() in ['true', '1', 'yes', 'on'] if target_reprocess is not None else False

    print(f"DEBUG: Configuration detected - Transformation: {target_transform}, Job ID: {target_load_id}, Reprocess: {reprocess}", flush=True)

    # run transforms
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