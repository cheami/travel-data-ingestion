from datetime import datetime
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# dag args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def reset_snowflake_db():
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
    
    # read sql
    with open('/opt/airflow/sql/reset_schemas.sql', 'r') as f:
        # clean comments
        sql_commands = "".join(line for line in f if not line.strip().startswith('--')).split(';')

    with conn.cursor() as cs:
        # set db
        db_name = Variable.get("snowflake_database")
        cs.execute(f"USE DATABASE {db_name}")
        
        # run commands
        for command in sql_commands:
            if command.strip():
                print(f"Executing: {command[:50]}...")
                cs.execute(command)
    
    conn.close()

# dag setup
with DAG(
    'reset_database',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Resets the Snowflake database by dropping and recreating schemas',
    tags=['maintenance'],
    is_paused_upon_creation=False,
) as dag:

    reset_schemas = PythonOperator(
        task_id='reset_schemas',
        python_callable=reset_snowflake_db
    )