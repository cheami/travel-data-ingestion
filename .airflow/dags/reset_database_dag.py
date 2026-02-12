from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    'reset_database',
    default_args=default_args,
    schedule_interval=None,
    description='Resets the database by dropping and recreating schemas',
    tags=['maintenance'],
    template_searchpath=['/opt/airflow/sql']
) as dag:

    reset_schemas = PostgresOperator(
        task_id='reset_schemas',
        postgres_conn_id='postgres_default',
        sql='reset_schemas.sql'
    )