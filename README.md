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
Run the following command to download images and start the containers:

```bash
docker compose up -d
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
    *   Example: `data/landing/fitbit/heart_rate/data.csv`
2.  **Trigger Ingestion**: Go to the Airflow UI, unpause, and trigger the `metadata_driven_ingestion` DAG. This loads data into the `bronze` schema.
3.  **Trigger Transformation**: Run the transformation DAGs to move data from `bronze` to `silver`.
4.  **Verify**: Check the Airflow logs or use Adminer to query tables in the `bronze` and `silver` schemas.
5.  **Reset (Optional)**: Trigger the `reset_database` DAG to drop all schemas and start fresh.

## Key Features

### Idempotency & Logging
The pipeline tracks every file ingestion attempt in the `admin.ingestion_logs` table.
*   **Prevents Duplicates**: If a file is logged as `SUCCESS`, it will be skipped in future runs.
*   **Traceability**: Each row in the target tables includes a `load_id` column that links back to the log entry. The log now also tracks `target_schema` and `target_table`.

### Schema Organization
Data is organized into the following schemas:
*   **admin**: System tables like `ingestion_logs`.
*   **bronze**: Raw data ingested directly from files (e.g., `bronze.google_timeline`, `bronze.fitbit_steps`).
*   **silver**: Cleaned and aggregated data (e.g., `silver.daily_spend`).
*   **gold**: Business-level aggregates (future).

### Configuration
The `configs/datasets.json` file dynamically controls source paths, file patterns, target schemas, and target tables.