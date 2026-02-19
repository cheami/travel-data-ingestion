from __future__ import annotations
from datetime import datetime, timedelta
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def execute_gold_procedure(procedure_name):
    """
    Connects to Snowflake and executes the given stored procedure.
    """
    print("Establishing connection to Snowflake...")
    conn = snowflake.connector.connect(
        user=Variable.get("snowflake_user"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse=Variable.get("snowflake_warehouse"),
        database=Variable.get("snowflake_database"),
        schema=Variable.get("snowflake_schema"),
        role=Variable.get("snowflake_role")
    )
    
    try:
        cursor = conn.cursor()
        query = f"CALL {procedure_name}()"
        print(f"Executing: {query}")
        cursor.execute(query)
        result = cursor.fetchall()
        print(f"Procedure execution result: {result}")
    finally:
        conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_to_gold',
    default_args=default_args,
    description='Triggers Gold layer stored procedures in Snowflake',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'snowflake'],
    is_paused_upon_creation=False,
) as dag:

    t1 = PythonOperator(
        task_id='call_full_travel_cost',
        python_callable=execute_gold_procedure,
        op_kwargs={'procedure_name': 'gold.SP_FULL_TRAVEL_COST'}
    )

    t2 = PythonOperator(
        task_id='call_travel_tax_report',
        python_callable=execute_gold_procedure,
        op_kwargs={'procedure_name': 'gold.SP_TRAVEL_TAX_REPORT'}
    )