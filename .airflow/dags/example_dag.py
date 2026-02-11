from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=["example"],
) as dag:
    bash_task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow!'",
    )