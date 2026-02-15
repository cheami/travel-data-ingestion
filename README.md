# Travel Data Ingestion Pipeline

This project is a metadata-driven data ingestion framework using Apache Airflow, PostgreSQL, and Docker. It simulates ingesting data from local files (representing Azure Blob Storage) into a Postgres database (representing the Bronze layer).

## Prerequisites

- Docker
- Docker Compose

## Technologies

- **Apache Airflow**: Orchestration engine for scheduling and monitoring workflows.
- **PostgreSQL**: Relational database used as the simulated Bronze layer.
- **Docker**: Containerization for consistent development environments.
- **Adminer**: Lightweight database management tool.

## Quick Start

### 1. Setup Environment
Create the `.env` file to ensure Docker uses your current user ID for file permissions. Run this in the project root:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Start Services
Run the following command to build the custom image (installing dependencies) and start the containers:

```bash
docker compose up --build -d
```

### 3. Check Status
Ensure all containers are running and healthy:

```bash
docker compose ps
```

---

## Accessing Services

### Apache Airflow (Orchestration)
- **URL:** http://localhost:8080
- **Username:** `admin`
- **Password:** `admin`

### Adminer (Database Viewer)
Use Adminer to query the Postgres database and view ingested tables.
- **URL:** http://localhost:8081
- **System:** `PostgreSQL`
- **Server:** `postgres`
- **Username:** `airflow`
- **Password:** `airflow`
- **Database:** `airflow`

---

## How to Run the Pipeline

1.  **Add Data**: Place your source files (CSV or JSON) in the `data/landing/` subdirectories defined in `configs/datasets.json`.
    *   Example: `data/landing/manual_logs/sample.csv`
    *   Example: `data/landing/transactions/data.csv`
    *   Example: `data/landing/fitbit/heart_rate/heart_rate_2023-08-04.csv`
2.  **Trigger Ingestion**: Go to the Airflow UI, unpause, and trigger the `metadata_driven_ingestion` DAG. This loads data into the `bronze` schema.
3.  **Trigger Transformation**: Run the `silver_transformation` DAG to move data from `bronze` to `silver`.
    *   **Parameters**:
        *   `transformation`: (Optional) Run a specific transformation (e.g., `fitbit_steps`).
        *   `job_id`: (Optional) Process a specific `load_id`.
        *   `Reprocess`: (Optional) Set to `True` to re-process data that has already been marked as `SUCCESS` in the transformation logs.
4.  **Verify**: Check the Airflow logs or use Adminer to query tables in the `bronze` and `silver` schemas.
5.  **Reset (Optional)**: Trigger the `reset_database` DAG to drop all schemas and start fresh.

## Key Features

### Idempotency & Logging
The pipeline tracks ingestion and transformation status to ensure data integrity.
*   **Ingestion**: Files are tracked in `admin.ingestion_logs`. If a file is logged as `SUCCESS`, it is skipped.
*   **Transformation**: Processed `load_id`s are tracked in `ADMIN.TRANSFORMATION_LOGS`.
    *   **Incremental**: Scripts check this log to skip already processed batches unless `Reprocess` is set to `True`.
    *   **Audit**: Accurate row counts are logged for every transformation run by querying the target Silver tables.
    *   **Traceability**: Each row in the target tables includes a `load_id` column that links back to the log entry.

### Modular Transformations
Transformation logic is organized in `scripts/transformations/` and processes data incrementally:
*   **Load-ID Based Processing**: Scripts iterate through distinct `load_id`s in the Bronze layer to process specific batches of data.
*   **Idempotency**: Existing data for a specific `load_id` is cleared from the Silver table before insertion to prevent duplicates.

# Google Timeline Transformation

## Overview
This module handles the parsing and transformation of Google Timeline (Location History) JSON data from the Bronze layer to the Silver layer.

## Source
*   **Table**: `bronze.google_timeline`
*   **Format**: JSON (Google Takeout Semantic Location History)

### Schema Organization
Data is organized into the following schemas:
*   **admin**: System tables like `ingestion_logs`.
*   **bronze**: Raw data ingested directly from files (e.g., `bronze.google_timeline`, `bronze.fitbit_steps`).
*   **silver**: Cleaned and aggregated data.
    *   **Finance**: `daily_spend`, `all_spending`
    *   **Health**: `hourly_step_count`, `sleep_log`, `sleep_daily_summary`, `heart_rate_minute_log`, `heart_rate_hourly_summary`
    *   **Travel**: `flight_logs`, `manual_logs`
*   **gold**: Business-level aggregates (future).

### Configuration
The `configs/datasets.json` file dynamically controls source paths, file patterns, target schemas, and target tables.