import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from ingestion_logic import load_config, ingest_dataset

# dag setup
with DAG(
    dag_id="metadata_driven_ingestion",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["ingestion", "metadata-driven"],
    max_active_tasks=1,
    is_paused_upon_creation=False,
) as dag:

    # get configs
    configs = load_config()

    # loop through datasets and make tasks
    for dataset_name, config in configs.items():
        PythonOperator(
            task_id=f"ingest_{dataset_name}",
            python_callable=ingest_dataset,
            op_kwargs={"dataset_name": dataset_name, "config": config},
        )