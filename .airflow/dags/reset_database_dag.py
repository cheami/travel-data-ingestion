from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import snowflake.connector

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

def reset_snowflake_db():
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
    
    # Read the SQL file
    with open('/opt/airflow/sql/reset_schemas.sql', 'r') as f:
        # Filter out lines starting with '--' to ignore comments
        sql_commands = "".join(line for line in f if not line.strip().startswith('--')).split(';')

    with conn.cursor() as cs:
        # Ensure we are in the correct database
        db_name = Variable.get("snowflake_database")
        cs.execute(f"USE DATABASE {db_name}")
        
        for command in sql_commands:
            if command.strip():
                print(f"Executing: {command[:50]}...")
                cs.execute(command)
    
    conn.close()

with DAG(
    'reset_database',
    default_args=default_args,
    schedule_interval=None,
    description='Resets the Snowflake database by dropping and recreating schemas',
    tags=['maintenance']
) as dag:

    reset_schemas = PythonOperator(
        task_id='reset_schemas',
        python_callable=reset_snowflake_db
    )