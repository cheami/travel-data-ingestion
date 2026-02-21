# Travel Data Engineering Pipeline

A metadata-driven ETL pipeline to ingest, transform, and analyze personal travel data from a multi-country gap year. Orchestrated with **Airflow**, processed in **Python**, and warehoused in **Snowflake**.

## Tech Stack

* **Orchestration:** Apache Airflow (Docker)
* **Warehouse:** Snowflake (Bronze/Silver/Gold architecture)
* **Compute:** Hybrid (Python for parsing/ingestion, Snowflake SQL for aggregation)
* **Storage:** Azure Blob Storage (Raw data landing)
* **Language:** Python 3.10 (Pandas, Snowflake Connector)

## Architecture

This project uses a **Metadata-Driven Ingestion** pattern. Instead of hardcoding DAGs or schema definitions in Python, the pipeline dynamically generates tasks by reading a config table (`ADMIN.FILE_DETAILS`).

**Schema Inference:** To handle file layouts, the pipeline inspects the **physical Bronze table** in Snowflake (`DESC TABLE`) at runtime. Instead of duplicating schema logic in a separate `file_columns` config, I chose to use the database DDL as the single source of truth for the expected structure.

**Flow:**

1. **Landing:** Raw CSV/JSON files upload to Azure Blob Storage.
2. **Bronze (Ingestion):** Airflow triggers dynamic ingestion. It reads the config for file patterns and **queries the Bronze table schema** to generate the correct `COPY INTO` statements.
3. **Silver (Transformation):** Python scripts clean, deduplicate, and normalize data (timestamps, JSON parsing).
4. **Gold (Reporting):** Stored Procedures aggregate business logic (e.g., trip costs, health recovery stats).

---

## Datasets

* **Google Timeline:** Raw JSON extracts of daily location history (manually verified).
* **Transactions:** Granular logs of every expense, categorized by type (Food, Travel, etc.).
* **Manual Log:** Master itinerary table (dates, locations, hotels).
* **Fitbit Data:** Physiological telemetry (Heart Rate, Steps, Sleep Scores) exported from wearable.
* **Flight Logs:** Aeronautical data exported from Flightradar24.

---

## Pipeline Components (DAGs)

### 1. `metadata_driven_ingestion`

Dynamically iterates through `ADMIN.FILE_DETAILS` to ingest files from Azure to Snowflake Bronze.

* **Idempotency:** Checks `ADMIN.INGESTION_LOGS` before loading. If a filename exists with `status='SUCCESS'`, it skips ingestion to prevent duplicates.
* **Schema Aware:** Dynamically maps file data to the target table by reading the table's columns from Snowflake metadata.

### 2. `silver_transformation`

Orchestrates Python-based transformations from Bronze to Silver.

* **Incremental Loading:** Reads `load_id`s from Bronze tables. Checks `ADMIN.TRANSFORMATION_LOGS` to only process new/unprocessed batches.
* **Logic:**
* **Google Timeline:** Parses nested JSON segments into relational rows.
* **Fitbit/Logs:** Standardizes timestamps, handles unit conversions, and deduplicates using `load_id`.
* **Idempotency:** Deletes existing rows for the current `load_id` in the target table before writing (DELETE/INSERT pattern).



### 3. `silver_to_gold`

Triggers Snowflake Stored Procedures to build analytical tables.

* Offloads heavy join/aggregation logic to Snowflake's compute engine.

### 4. `full_e2e_pipeline`

Orchestrator DAG. Triggers the dependency chain: `Ingestion` -> `Silver` -> `Gold` using `TriggerDagRunOperator`.

### 5. `reset_database`

Utility DAG. Drops and recreates `BRONZE`, `SILVER`, and `GOLD` schemas for clean re-runs.

---

## Gold Reports (Stored Procedures)

### 📊 Full Travel Cost (`SP_FULL_TRAVEL_COST`)

Joins **Manual Logs** (itinerary) with **Transactions** (spend) to create a daily financial ledger.

* **Logic:** Pivots transaction types (Hotel, Food, Activity) into columns.
* **Output:** Calculates daily totals, running totals, and aggregates comments for high-spend days.

### 🏥 Travel Tax Report (`SP_TRAVEL_TAX_REPORT`)

Analyzes the physiological toll of travel by correlating flight data with health metrics.

* **Logic:** Joins **Flight Logs** with next-day **Fitbit Sleep/Heart Rate** data.
* **Metrics:** Calculates "Recovery Status" based on flight duration (>4 hours) vs. subsequent deep sleep and heart rate variability.

---

## Streamlit Dashboards

This repository includes two Streamlit applications located in the `streamlit/` directory to visualize travel data stored in Snowflake.

### 1. Daily Travel Summary
**File:** `streamlit/daily_travel_summary.py`

A dashboard designed to provide a high-level summary of a specific travel date. It calls a stored procedure (`TRAVEL_DATA.GOLD.SP_GET_DAILY_TRAVEL_SUMMARY`) to retrieve aggregated data.

**Key Features:**
*   **Top Level Metrics:** Displays total money spent, total steps taken, and sleep score for the day.
*   **Logs & Flights:** Lists manual journal entries and flight details if available.
*   **Spending Breakdown:** Visualizes expenses by category using a bar chart and detailed table.
*   **Geospatial Timeline:** Shows a map of the day's movements (visits and activities) alongside a segment-by-segment timeline.

### 2. Travel & Movement Map
**File:** `streamlit/travel_and_movement_map.py`

An advanced analytics and mapping dashboard that operates over a selected date range. It queries Silver-layer tables directly to render complex visualizations using PyDeck.

**Key Features:**
*   **Interactive Map:** Renders travel routes with specific visualizations:
    *   **Color-coded Paths:** Different colors for Walking, Driving, Trains/Buses, and Flying.
    *   **Flight Arcs:** 3D arcs representing flights between locations.
*   **Itinerary & Analytics:**
    *   **Daily Itinerary:** A collapsible list of daily activities, hotel stays, and transactions.
    *   **Spending Tab:** Daily spending trends and top expensive transactions.
    *   **Movement Tab:** Daily step counts and distance traveled by transport mode.
    *   **Health Tab:** Trends for sleep scores and resting heart rate.

---

## Setup & Run

### 1. Environment

Create a `.env` file for Docker permissions and start the stack:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d --build

```

### 2. Snowflake Config

In Airflow UI (**Admin > Variables**), set your Snowflake credentials:

* `snowflake_account`, `snowflake_user`, `snowflake_password`
* `snowflake_warehouse`, `snowflake_role`
* `snowflake_database` (Default: `TRAVEL_DATA`)

### 3. Azure Integration

1. Create a Storage Integration and Stage in Snowflake pointing to your Azure Blob Container.
2. Grant usage permissions to your Airflow Snowflake role.

### 4. Initialization

1. Run the **`reset_database`** DAG to initialize schemas and logging tables.
2. Populate `ADMIN.FILE_DETAILS` in Snowflake with your file paths/patterns.
3. Upload raw data to the Azure container.

### 5. Execution

Trigger **`full_e2e_pipeline`** to run the full ETL process.