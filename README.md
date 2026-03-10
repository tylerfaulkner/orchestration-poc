# NYC Yellow Taxi - Orchestration PoC

A proof-of-concept data pipeline demonstrating end-to-end orchestration across multiple platforms using NYC Yellow Taxi trip data. The pipeline is designed to run **monthly**, operates **incrementally**, and is **inherently idempotent** — re-running any step produces the same result without duplicating data.

This repository serves as a reference implementation for evaluating orchestration platforms against a realistic, multi-system data pipeline.

---

## Architecture Overview

The pipeline flows left-to-right through five major stages:

```
Databricks (Ingest) → ADLS Gen2 (Lake) → Snowflake EDL (Raw) → Snowflake Curated (DWH) → Snowflake MRT (Data Products) → Export
```

Each stage is loosely coupled through storage layers (ADLS Gen2, Snowflake), making individual components independently testable and replaceable — a key requirement for evaluating different orchestration tools.

### Authentication and Secrets

All service-to-service authentication flows through **Azure Key Vault**. Each stage uses the appropriate auth mechanism:

| Connection | Auth Method |
|---|---|
| Databricks → ADLS Gen2 | Service Account (via Key Vault) |
| Snowflake → ADLS Gen2 (Catalog Integration) | Service Account (via Key Vault) |
| Export Databricks → ADLS Gen2 | Service Account (via Key Vault) |
| Databricks API (orchestrator trigger) | PAT Auth (via Key Vault) |
| Export → Azure File Share | Account Token (via Key Vault) |

### Environment Isolation

Each engineer operates in an isolated environment context based on their name. Schemas are dynamically prefixed (e.g., `KENT_EDL`, `KENT_CURATED`, `KENT_MRT`), allowing parallel development without conflicts — all driven by a single `env` variable.

---

## Pipeline Stages

### Stage 1: Data Ingestion (Databricks + ADLS Gen2)

Databricks notebooks ingest raw data from external sources into Azure Data Lake Storage Gen2 as Delta Lake tables with Apache Iceberg metadata.

| Notebook | Purpose | Frequency |
|---|---|---|
| `source_taxi_zone_lookup.ipynb` | Loads 265 NYC taxi zone reference records | One-time / Periodic |
| `source_yellow_tripdata.ipynb` | Ingests monthly Yellow Taxi `.parquet` files from the NYC TLC | Monthly (parameterized by Year/Month) |

**Key details:**
- Source data is downloaded from the NYC TLC CloudFront distribution
- Files land in an ADLS Gen2 inbound container
- Unity Catalog Delta tables are created with partitioning by `trip_year` / `trip_month`
- Audit columns (execution ID, timestamp, source file) are appended for lineage

### Stage 2: Enterprise Data Lake — EDL (Snowflake)

Snowflake accesses the ADLS-hosted Delta/Iceberg tables through a **Catalog Integration** and **Object Store** configuration, exposing them as external Iceberg tables in the EDL schema.

| Table | Description |
|---|---|
| `YELLOW_TAXI_TRIPS` | All ingested Yellow Taxi trip records (Iceberg external table) |
| `TAXI_ZONE_LOOKUP` | Taxi zone reference data (Iceberg external table) |

The EDL layer provides a read-only, schema-on-read view of the lake data without copying it into Snowflake native storage.

### Stage 3: Curated Data Warehouse — DWH_CURATED (Snowflake + dbt)

dbt Core transforms raw EDL data into clean, enriched, analytics-ready tables through a staged approach.

#### Staging (Views)

| Model | Description |
|---|---|
| `stg_yellow_taxi_trips` | Cleans raw trip data: renames columns, casts types, deduplicates, generates surrogate `trip_id` |
| `stg_taxi_zone_lookup` | Trims and standardizes zone reference data |

#### Curated (Tables)

| Model | Materialization | Description |
|---|---|---|
| `dim_location` | Table (full rebuild) | Enriched taxi zones with flags: `is_airport`, `is_yellow_zone`, `borough_group`, Manhattan sub-regions |
| `fact_yellow_taxi_trips` | Incremental (merge) | One row per trip with 30+ calculated metrics, boolean flags (`is_rush_hour`, `is_weekend`, `is_late_night`, `is_airport_trip`), and referential integrity to all dimension/seed tables |

**Seed tables** provide slowly-changing reference dimensions:

| Seed | Records | Description |
|---|---|---|
| `vendor.csv` | 4 | Taxi technology vendors |
| `rate_code.csv` | 8 | Fare rate classifications |
| `payment_type.csv` | 7 | Payment method types |
| `store_and_fwd_flag.csv` | 2 | Store-and-forward indicator values |

### Stage 4: Data Products — MRT (Snowflake + dbt)

Mart-level aggregate tables serve specific analytical use cases. All are **incremental with merge strategy**.

| Model | Grain | Use Case |
|---|---|---|
| `agg_daily_zone_summary` | Date x Pickup Zone | Operational dashboards, zone heatmaps |
| `agg_hourly_demand` | Date x Hour x Borough | Demand forecasting, shift scheduling, surge pricing |
| `agg_monthly_zone_summary` | Month x Pickup Zone | Trend analysis, year-over-year comparisons |
| `agg_vendor_performance` | Month x Vendor | Vendor scorecards, performance monitoring |

### Stage 5: Export (Databricks + ADLS Gen2)

The final stage exports the `AGG_MONTHLY_ZONE_SUMMARY` aggregate for downstream consumption:

1. **Databricks** converts the monthly zone summary to CSV (via `export_aggregate_data.ipynb`)
2. The CSV is written to **ADLS Gen2**
3. The file is then copied to an **Azure File Share** for consumer access

This stage is also parameterized by environment and date range.

---

## Incremental and Idempotent Design

The pipeline is designed so that every run processes only new data and can be safely re-executed:

| Layer | Strategy | Watermark |
|---|---|---|
| Ingestion (Databricks) | Parameterized by month/year; overwrites partition on re-run | Year + Month parameter |
| `fact_yellow_taxi_trips` | Incremental merge on `trip_id` | `pickup_datetime > max(pickup_datetime)` |
| `agg_daily_zone_summary` | Incremental merge on composite key | `pickup_date > max(pickup_date)` |
| `agg_hourly_demand` | Incremental merge on composite key | `pickup_date > max(pickup_date)` |
| `agg_monthly_zone_summary` | Incremental merge on composite key | `date_from_parts(year, month, 1) > max(pickup_date)` |
| `agg_vendor_performance` | Incremental merge on composite key | `date_from_parts(year, month, 1) > max(pickup_date)` |

- **First run**: Performs a full load (no existing watermark).
- **Subsequent runs**: Only processes records beyond the current high-water mark.
- **Re-runs**: The merge strategy (upsert) ensures duplicate executions update existing rows rather than creating duplicates.

---

## Data Quality and Testing

dbt models include comprehensive data tests at every layer:

- **Uniqueness**: `trip_id` (fact), `location_id` (dimension)
- **Not-null**: Critical fields (timestamps, location IDs, fare amounts)
- **Referential integrity**: Foreign key relationships to vendor, rate code, payment type, and location seeds
- **Accepted values**: Day-of-week names, trip borough types, borough groups
- **Implausible record filtering**: Negative fares, trips exceeding 24 hours, and trips exceeding 500 miles are excluded at the fact layer

---

## Repository Structure

```
orchestration-poc/
├── databricks/                          # Databricks notebooks (JSON format)
│   ├── source_yellow_tripdata.ipynb     # Monthly taxi trip ingestion
│   ├── source_taxi_zone_lookup.ipynb    # Zone reference data load
│   └── export_aggregate_data.ipynb      # Export aggregates to ADLS/File Share
├── snowflake/                           # dbt project for Snowflake transformations
│   ├── models/
│   │   ├── staging/                     # Staging views (raw → clean)
│   │   ├── curated/                     # Dimension and fact tables
│   │   └── mrt/                         # Mart aggregate tables
│   ├── seeds/                           # Reference dimension CSVs
│   ├── macros/                          # dbt macros (schema generation)
│   ├── dbt_project.yml                  # dbt project configuration
│   ├── profiles.yml                     # Snowflake connection profile
│   └── packages.yml                     # dbt package dependencies
├── sql_scripts/                         # Manual SQL utilities
│   ├── Create EDL Tables.sql            # Iceberg external table DDL
│   └── Create Export File.sql           # COPY INTO export script
├── docs/                                # Architecture diagram and data dictionary
├── pyproject.toml                       # Python project config (dbt deps)
└── uv.lock                              # Dependency lock file
```

---

## Prerequisites

- **Python** 3.11+
- **dbt-core** 1.11.7+ with **dbt-snowflake** 1.11.3+
- **Snowflake** account with access to `WBMIQA_ORCHPOC_DB`
- **Azure Databricks** workspace with Unity Catalog enabled
- **Azure ADLS Gen2** storage account with appropriate container structure
- **Azure Key Vault** access for secrets management

---

## Getting Started

### 1. Install Dependencies

```bash
uv sync
```

### 2. Configure Environment

Set your environment prefix (used for schema isolation):

```yaml
# In profiles.yml, the schema is driven by the env variable
# e.g., env=KENT produces schemas: KENT_EDL, KENT_CURATED, KENT_MRT
```

### 3. Run dbt

```bash
cd snowflake

# Install dbt packages
dbt deps

# Load seed reference data
dbt seed

# Run all models
dbt run

# Run tests
dbt test
```

### 4. Execute Databricks Notebooks

Run notebooks in order, parameterized for your target month:

1. `source_taxi_zone_lookup.ipynb` — One-time zone reference load
2. `source_yellow_tripdata.ipynb` — Monthly trip data ingestion (set YEAR/MONTH)
3. `export_aggregate_data.ipynb` — Export aggregates after dbt completes

---

## Monthly Execution Flow

A typical monthly orchestration run follows this sequence:

1. **Ingest** — Databricks runs `source_yellow_tripdata.ipynb` for the target month
2. **Transform** — dbt incrementally builds staging, curated, and mart layers
3. **Test** — dbt runs data quality tests across all layers
4. **Export** — Databricks exports the monthly zone summary to CSV on Azure File Share

The orchestration platform under evaluation is responsible for coordinating these steps, passing parameters (month/year, environment), handling dependencies between stages, managing retries, and providing observability.
