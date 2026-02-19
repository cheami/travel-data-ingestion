from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from transformation_logic import transform_silver

with DAG(
    dag_id="silver_transformation",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["transformation", "silver"],
    is_paused_upon_creation=False,
    params={
        "transformation": Param(None, type=["null", "string"], description="Optional: Specific transformation to run (e.g., 'transactions, manual_logs, flight_logs, fitbit_steps, fitbit_sleep_score, fitbit_heart_rate, google_timeline')."),
        "job_id": Param(None, type=["null", "integer"], description="Optional: Specific Job ID (load_id) to process."),
        "Reprocess": Param(False, type="boolean", description="If True, reprocesses all load_ids regardless of status."),
    },
) as dag:

    transform_task = PythonOperator(
        task_id="run_silver_transformations",
        python_callable=transform_silver,
    )
