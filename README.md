# Logistics Data Engineering Pipeline

## Overview
![Logistics Data Engineering and SCD Type 2 Pipeline](./Logistics%20Data%20Engineering%20and%20SCD%20Type%202%20Pipeline.png)

 Azure data factory incremental ingestion  to blob storage and snowflake : https://github.com/InimfonabasiUdofia/Logistics_Pipeline_Azure.git

This project implements an end-to-end data engineering pipeline for a logistics platform using **Azure Data Factory (ADF), Azure Blob Storage, Snowflake, dbt, and Apache Airflow**.

The pipeline incrementally ingests logistics data, stores the extracted data in Azure Blob Storage, loads it into Snowflake, and transforms it into analytics-ready datasets using the medallion architecture.

Apache Airflow orchestrates the pipeline execution, ensuring that data ingestion, freshness checks, and dbt transformations run in the correct sequence.

The project focuses on incremental data loading, change tracking using update timestamps, data transformation, and automated workflow orchestration.

## Architecture

The pipeline follows a medallion architecture consisting of Bronze, Silver, and Gold layers.

```text
                Source Logistics Data
                         |
                         v
              Azure Data Factory
                (Incremental CDC)
                         |
                         v
                Azure Blob Storage
             (Incremental Data Files
              + Maximum Updated At)
                         |
                         v
              Azure Data Factory
                 (Snowflake Load)
                         |
                         v
                  SNOWFLAKE
                         |
                         v
                  Bronze Layer
                (Raw Ingested Data)
                         |
                         v
                    dbt Models
                         |
                         v
                  Silver Layer
             (Cleaned and Transformed)
                         |
                         v
                    dbt Models
                         |
                         v
                   Gold Layer
              (Analytics-Ready Data)
                         ^
                         |
                 Apache Airflow
                (Orchestration)
```

## Technologies Used

- **Azure Data Factory:** Extracts incremental data from source systems and orchestrates data movement into Azure Blob Storage and Snowflake.
- **Azure Blob Storage:** Stores extracted incremental data files and the maximum update timestamp used to track ingestion progress.
- **Snowflake:** Provides cloud data warehousing and storage for the Bronze, Silver, and Gold data layers.
- **dbt:** Implements SQL-based data transformations and incremental models.
- **Apache Airflow:** Orchestrates the ingestion and transformation workflow.
- **Astro:** Provides the Airflow development and execution environment.
- **Python:** Defines the Airflow DAG and task dependencies.
- **SQL and Jinja:** Define dbt transformations and incremental loading logic.

## Data Ingestion and Incremental Loading

Azure Data Factory handles the incremental ingestion process.

Rather than extracting the entire dataset on every run, the pipeline uses an update timestamp to identify records that have changed since the previous successful ingestion.

### Incremental ingestion process

1. Retrieve the maximum previously processed update timestamp.
2. Use the timestamp as the watermark for the next extraction.
3. Extract new or updated records from the source system.
4. Store the incremental data in Azure Blob Storage.
5. Persist the maximum update timestamp for the current extraction.
6. Transfer the extracted data from Blob Storage into Snowflake using Azure Data Factory.
7. Load the ingested data into the Snowflake Bronze layer.
8. Transform and incrementally process the data through dbt models into the Silver and Gold layers.

The maximum update timestamp is used as a watermark to support incremental processing and reduce unnecessary data extraction.

**Important:** The watermark should only advance after the corresponding batch has been successfully persisted, and the pipeline should support retries and duplicate handling to avoid missing records or advancing past a failed batch.

## Snowflake Medallion Architecture

### 1. Bronze Layer — Raw Data

The Bronze layer stores data ingested from Azure Blob Storage with minimal transformation.

It preserves the source data for traceability, recovery, and downstream processing.

Example datasets include:

- Couriers
- Customers
- Shipments
- Tracking events

### 2. Silver Layer — Cleaned and Transformed Data

The Silver layer contains cleaned, structured, and incrementally updated datasets created using dbt.

The dbt models apply SQL transformations, select relevant columns, and use unique keys to maintain the latest records.

Examples include:

| Model | Unique key | Incremental timestamp |
|---|---|---|
| Couriers | `COURIER_ID` | `COURIER_UPDATED_AT` |
| Customers | `CUSTOMER_ID` | `CUSTOMER_UPDATED_AT` |
| Shipments | `SHIPMENT_ID` | `SHIPMENT_UPDATED_AT` |
| Tracking events | `EVENT_ID` | `TRACKING_UPDATED_AT` |

The models use `is_incremental()` to apply timestamp filters during incremental runs. The `unique_key` configuration identifies records for incremental updates.

### 3. Gold Layer — Analytics-Ready Data

The Gold layer contains business-oriented datasets designed for reporting and analytical queries.

A shipment analytics model can combine shipment, courier, and tracking information to support metrics such as:

- Shipment status and delivery performance
- Courier activity
- Customer shipping activity
- Shipping fees
- Shipment tracking history
- Promised versus actual delivery times

These models can be used by reporting tools and downstream analytics applications.

## dbt Transformations

dbt manages the SQL transformation process across the Snowflake data warehouse.

The project uses incremental models to process records based on update timestamps rather than rebuilding entire datasets on every execution.

Example incremental dbt model:

```sql
{{
    config(
        materialized='incremental',
        unique_key='COURIER_ID'
    )
}}

SELECT
    COURIER_ID,
    COURIER_NAME,
    SERVICE_TYPE,
    COURIER_UPDATED_AT
FROM {{ ref('couriers') }}

{% if is_incremental() %}
WHERE COURIER_UPDATED_AT > (
    SELECT COALESCE(
        MAX(COURIER_UPDATED_AT),
        '1900-01-01'
    )
    FROM {{ this }}
)
{% endif %}
```

The model selects records from the upstream `couriers` relation and, during incremental runs, filters for records with update timestamps newer than the maximum timestamp already present in the target model.

Similar logic is applied to customers, shipments, and tracking events.

The Gold layer can contain joined datasets that combine shipments and tracking events for business reporting.

**Note:** For production workloads, consider handling records with identical timestamps, late-arriving updates, deleted records, and overlapping extraction windows. A timestamp-only `>` filter can miss changes that share the current maximum timestamp.

## Apache Airflow Orchestration

Apache Airflow manages the execution sequence using a DAG named `orchestrate`.

The DAG is configured with manual triggering (`schedule=None`) and disables historical catchup.

### Workflow tasks

| Task | Description |
|---|---|
| `run_adf_pipeline` | Triggers the Azure Data Factory pipeline and waits for it to finish. |
| `clean_target` | Removes previous dbt artifacts from the project directory. |
| `source_freshness` | Installs dbt dependencies and checks source freshness. |
| `build_dbt` | Executes dbt models and associated tests using `dbt build`. |

### Execution order

```text
run_adf_pipeline
       |
       v
  clean_target
       |
       v
 source_freshness
       |
       v
    build_dbt
```

The workflow ensures that the ADF pipeline completes before dbt transformations begin.

The `dbt build` command builds the configured models and runs applicable tests and other supported resources according to the dbt project configuration.

## Project Structure

A suggested repository structure is:

```text
logistics-data-pipeline/
├── dags/
│   └── orchestrate.py
├── logistic/
│   ├── models/
│   │   ├── silver/
│   │   │   ├── couriers.sql
│   │   │   ├── customers.sql
│   │   │   ├── shipments.sql
│   │   │   └── tracking_events.sql
│   │   └── gold/
│   │       └── shipment_analytics.sql
│   ├── tests/
│   ├── macros/
│   ├── dbt_project.yml
│   └── packages.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

The structure above is illustrative; adapt the directory names to match your actual repository.

## Configuration and Prerequisites

Before running the pipeline, configure the following:

- An Azure Data Factory instance with the required pipelines and linked services.
- An Azure Blob Storage account and container for incremental data files and watermark metadata.
- A Snowflake account, warehouse, database, schemas, and required permissions.
- An Airflow environment, such as Astro.
- An Airflow connection named `azure_data_factory` configured for the Azure Data Factory provider.
- A dbt project configured to connect to Snowflake.
- The required Python packages and Airflow providers.

Keep credentials and sensitive configuration in Airflow connections, environment variables, or an appropriate secrets manager. Do not commit credentials to version control.

## Running the Pipeline

### 1. Prepare the dbt project

Navigate to the dbt project directory and install dependencies:

```bash
cd logistic
dbt deps
```

Configure the Snowflake connection using the appropriate dbt profile or environment variables.

### 2. Validate source freshness

```bash
dbt source freshness
```

This checks freshness for sources configured in the dbt project.

### 3. Run dbt transformations

```bash
dbt build
```

This builds the configured models and runs applicable tests.

### 4. Trigger the Airflow DAG

Start the Astro Airflow environment and trigger the `orchestrate` DAG through the Airflow interface or CLI.

Airflow will execute the configured ADF pipeline, perform the dbt preparation and freshness checks, and then run the dbt build.

## Reliability and Data Quality

The pipeline is designed around incremental ingestion and orchestrated transformations. For reliable production operation, consider implementing:

- **Watermark management:** Store the extraction watermark durably and update it only when the associated batch has been safely persisted.
- **Idempotent loading:** Ensure that retries do not create duplicate records.
- **Incremental merge logic:** Use stable unique keys to update existing records and insert new ones.
- **Data quality tests:** Add dbt `not_null`, `unique`, and relationship tests where appropriate.
- **Monitoring and logging:** Track ADF pipeline runs, Airflow task statuses, and dbt execution results.
- **Late-arriving records:** Use a lookback window or a composite watermark where timestamp collisions are possible.
- **Recovery:** Retain raw files and batch metadata so failed loads can be replayed.
- **Security:** Use least-privilege access, managed identities where supported, and secure credential storage.

## Conclusion

This project demonstrates an end-to-end cloud data engineering workflow that integrates Azure Data Factory, Azure Blob Storage, Snowflake, dbt, and Apache Airflow.

By combining watermark-based incremental ingestion, a Snowflake medallion architecture, incremental dbt transformations, and DAG-based orchestration, the pipeline provides a foundation for maintaining reliable and analytics-ready logistics data.

The resulting datasets support operational reporting and further analysis of customers, couriers, shipments, and tracking events.
