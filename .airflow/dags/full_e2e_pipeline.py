from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# dag args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# pipeline setup
with DAG(
    dag_id='full_e2e_pipeline',
    default_args=default_args,
    description='Orchestrates the full pipeline: Ingestion -> Silver -> Gold',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['e2e', 'orchestration'],
    is_paused_upon_creation=False,
) as dag:

    # trigger ingestion
    ingestion_task = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='metadata_driven_ingestion',
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True
    )

    # trigger silver
    silver_task = TriggerDagRunOperator(
        task_id='trigger_silver_transformation',
        trigger_dag_id='silver_transformation',
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True,
        execution_timeout=timedelta(minutes=60)
    )

    # trigger gold
    gold_task = TriggerDagRunOperator(
        task_id='trigger_silver_to_gold_transformation',
        trigger_dag_id='silver_to_gold',
        wait_for_completion=True,
        poke_interval=20,
        reset_dag_run=True
    )

    # run order
    ingestion_task >> silver_task >> gold_task