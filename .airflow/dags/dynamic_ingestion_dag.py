from __future__ import annotations

import json
import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Path to the configuration file inside the container
CONFIG_PATH = "/opt/airflow/configs/datasets.json"
DATA_PATH = "/opt/airflow/data/landing"

def load_config():
    """Loads the dataset configuration from JSON."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

def ingest_dataset(dataset_name, config, **kwargs):
    """
    Simulates the ingestion logic.
    In the future, this will connect to Azure Blob and Snowflake.
    For now, it scans the local 'landing' folder.
    """
    source_subfolder = config.get("source_path")
    file_pattern = config.get("file_pattern")
    
    full_path = os.path.join(DATA_PATH, source_subfolder)
    
    print(f"--- Starting Ingestion for {dataset_name} ---")
    print(f"Looking for files in: {full_path}")
    print(f"Matching pattern: {file_pattern}")
    
    # Simulation of checking for files
    if os.path.exists(full_path):
        files = [f for f in os.listdir(full_path) if f.endswith(file_pattern.replace("*", ""))]
        if files:
            print(f"Found {len(files)} files to process: {files}")
            # TODO: Add logic here to read file and write to Bronze layer (Parquet/Postgres)
        else:
            print("No new files found.")
    else:
        print(f"Directory {full_path} does not exist. Please create it and add data.")

    print(f"Target Table: {config.get('target_table')}")
    print("--- Ingestion Complete ---")


# Define the DAG
with DAG(
    dag_id="metadata_driven_ingestion",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["ingestion", "metadata-driven"],
) as dag:

    configs = load_config()

    for dataset_name, config in configs.items():
        PythonOperator(
            task_id=f"ingest_{dataset_name}",
            python_callable=ingest_dataset,
            op_kwargs={"dataset_name": dataset_name, "config": config},
        )