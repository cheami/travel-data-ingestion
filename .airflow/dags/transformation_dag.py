from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from transformation_logic import transform_silver

with DAG(
    dag_id="silver_transformation",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["transformation", "silver"],
) as dag:

    transform_task = PythonOperator(
        task_id="run_silver_transformations",
        python_callable=transform_silver,
    )
    
    transform_task