# NYC Yellow Taxi — dbt Project (Snowflake)

## Overview

This dbt project builds an **analytical dataset** from raw NYC Yellow Taxi trip data stored in Snowflake. It transforms two source tables into clean, enriched, and pre-aggregated models ready for dashboards, reporting, and data science.

All schema names are driven by an **environment variable** (`env`), making it easy to run the same project against isolated dev, QA, or production schemas.

### Environment-Driven Schemas

The `env` variable (e.g. `KENT`, `QA`, `PROD`) controls where everything is read from and written to:

| Layer | Schema Pattern | Example (`env: KENT`) |
|-------|---------------|----------------------|
| **Source** (raw data) | `{env}_EDL` | `KENT_EDL` |
| **Seeds / Staging / Dim / Fact** | `{env}_CURATED` | `KENT_CURATED` |
| **Aggregates** | `{env}_MRT` | `KENT_MRT` |

All schemas live in database `WBMIQA_ORCHPOC_DB`. dbt will auto-create schemas (`CREATE SCHEMA IF NOT EXISTS`) on the first run.

---

## What Gets Built

### Source Tables (read-only)

| Table | Description |
|-------|-------------|
| `{env}_EDL.YELLOW_TAXI_TRIPS` | NYC TLC Yellow Taxi trip records. Each row is one metered taxi trip with timestamps, locations, fare components, and coded fields. New data arrives monthly. |
| `{env}_EDL.TAXI_ZONE_LOOKUP` | Reference table mapping 265 TLC Taxi Zone IDs to borough, zone name, and service zone. |

### Seeds (Reference Data)

Four CSV files decode the integer/flag codes in the trip data into human-readable labels. These are loaded as tables in `{env}_CURATED`.

| Seed | Decodes |
|------|---------|
| `vendor.csv` | `VendorID` → vendor name (Creative Mobile Technologies, Curb Mobility, Myle Technologies, Helix) |
| `rate_code.csv` | `RatecodeID` → rate code name (Standard, JFK, Newark, Nassau/Westchester, Negotiated, Group Ride) |
| `payment_type.csv` | `payment_type` → payment method (Credit Card, Cash, Flex Fare, No Charge, Dispute, Unknown, Voided) |
| `store_and_fwd_flag.csv` | `store_and_fwd_flag` → description (Y/N store-and-forward indicator) |

### Staging Models → `{env}_CURATED` (Views)

| Model | Description |
|-------|-------------|
| `stg_yellow_taxi_trips` | Renames all columns to snake_case, casts types explicitly, deduplicates identical rows, and generates a surrogate `trip_id`. No row filtering — that happens in the curated layer. |
| `stg_taxi_zone_lookup` | Renames and trims the zone lookup reference table. |

### Curated Models → `{env}_CURATED` (Tables)

| Model | Grain | Description |
|-------|-------|-------------|
| `dim_location` | One row per zone | Enriched location dimension with `is_airport`, `is_yellow_zone`, `borough_group` (Manhattan / Outer Borough / New Jersey / Other), and `manhattan_subregion` flags. Full table rebuild (~265 rows). |
| `fact_yellow_taxi_trips` | One row per trip | Core enriched fact table. Joins trips with all seeds and location dimension for both pickup and dropoff. Adds calculated fields: `trip_duration_minutes`, `avg_speed_mph`, `tip_percentage`, `fare_per_mile`, `fare_per_minute`, `is_rush_hour`, `is_weekend`, `is_late_night`, `is_airport_trip`, `trip_borough_type`. Filters out implausible records (negative fares, >24hr trips, >500mi). |

### MRT Models → `{env}_MRT` (Tables)

| Model | Grain | Description |
|-------|-------|-------------|
| `agg_daily_zone_summary` | Date × Zone | Daily aggregation by pickup zone. Trip counts, revenue totals/averages, payment mix, rush hour share. For operational dashboards and zone-level heatmaps. |
| `agg_monthly_zone_summary` | Month × Zone | Monthly aggregation by pickup zone. Same metrics as daily plus `active_days`, `avg_daily_trips`, weekend/late-night trip counts, and credit card percentage. For month-over-month trend analysis. |
| `agg_hourly_demand` | Date × Hour × Borough | Hourly demand patterns by borough. Trip counts, avg speed (congestion proxy), airport trips, cross-borough movement. For demand forecasting and shift scheduling. |
| `agg_vendor_performance` | Month × Vendor | Monthly vendor scorecard. Volume, revenue efficiency, tipping, credit card vs cash split, store-and-forward rate (connectivity health), airport and rush hour mix. |

### DAG

```
{env}_EDL.YELLOW_TAXI_TRIPS ─► stg_yellow_taxi_trips ─┐
{env}_EDL.TAXI_ZONE_LOOKUP  ─► stg_taxi_zone_lookup ──► dim_location ──┐
                                                                        │
vendor ────────────────────────────────► fact_yellow_taxi_trips          │
rate_code ─────────────────────────────►       │                        │
payment_type ──────────────────────────►       ├──► agg_daily_zone_summary
store_and_fwd_flag ────────────────────►       ├──► agg_monthly_zone_summary
                                               ├──► agg_hourly_demand
                                               └──► agg_vendor_performance

  {env}_CURATED                                       {env}_MRT
  ─────────────                                       ─────────
  seeds (4 tables)                                    agg_daily_zone_summary
  stg_yellow_taxi_trips (view)                        agg_monthly_zone_summary
  stg_taxi_zone_lookup (view)                         agg_hourly_demand
  dim_location                                        agg_vendor_performance
  fact_yellow_taxi_trips
```

---

## How Incremental Models Work

All curated and MRT models except `dim_location` use **incremental materialization with a merge strategy**. This means dbt only processes **new rows** instead of rebuilding entire tables on every run.

### First Run (Full Load)

On the very first `dbt build`, each incremental table doesn't exist yet. dbt runs the **full query** (the `{% if is_incremental() %}` block is skipped) and creates the table — just like a normal `table` materialization.

### Subsequent Runs (Incremental)

On every run after that, `is_incremental()` returns `true` and the **watermark filter** kicks in. For example, in `fact_yellow_taxi_trips`:

```sql
select * from stg_yellow_taxi_trips
where pickup_datetime > (select max(pickup_datetime) from fact_yellow_taxi_trips)
```

This finds the latest `pickup_datetime` already loaded (e.g. `2020-01-31 23:59:59`), then only selects rows **after** that timestamp. The new rows are then merged into the existing table using Snowflake's `MERGE INTO`:

- **Matching rows** (same `unique_key`) → updated
- **New rows** → inserted

### Watermark & Merge Keys

Each model declares what makes a row unique and how to detect new data:

| Model | Watermark (filter) | Merge Key (`unique_key`) |
|-------|-------------------|-------------------------|
| `fact_yellow_taxi_trips` | `pickup_datetime` | `trip_id` |
| `agg_daily_zone_summary` | `pickup_date` | `(pickup_date, pickup_zone)` |
| `agg_monthly_zone_summary` | Year/month derived | `(pickup_year, pickup_month, pickup_zone)` |
| `agg_hourly_demand` | `pickup_date` | `(pickup_date, pickup_hour, pickup_borough)` |
| `agg_vendor_performance` | Year/month derived | `(pickup_year, pickup_month, vendor_id)` |

For the **MRT aggregate tables**, this is especially important — when February data arrives, the merge recalculates and **upserts** the February aggregate rows without touching January's data.

### Data Flow on Monthly Loads

```
Monthly parquet load → {env}_EDL.YELLOW_TAXI_TRIPS
                              ↓
                    stg_yellow_taxi_trips (view — always current)
                              ↓
              fact_yellow_taxi_trips (only new rows via pickup_datetime watermark)
                     ↓          ↓          ↓          ↓
               agg_daily   agg_monthly  agg_hourly  agg_vendor
              (each filters from fact using its own watermark)
```

### Full Refresh

If you ever need to rebuild from scratch (schema change, bad data, etc.), use the `--full-refresh` flag:

```bash
dbt build --full-refresh --vars '{env: KENT}' --profiles-dir .
```

This drops and recreates all incremental tables as if it were the first run.

---

## Prerequisites

- Python 3.10+
- A Snowflake account with access to `WBMIQA_ORCHPOC_DB`

### Install dbt

```bash
uv sync
.venv\Scripts\activate          # Windows
```

Verify the installation:

```bash
dbt --version
```

---

## Setup

### 1. Create a local `profiles.yml`

Create a file called `profiles.yml` **in this folder** (`snowflake/`). This file is git-ignored so your credentials stay out of version control.

The `schema` value in profiles.yml is used as a fallback — the `generate_schema_name` macro overrides it with `{env}_CURATED` or `{env}_MRT` based on the model config. You can set it to any valid schema name.

#### Username / Password

```yaml
snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-account-id>       # e.g. xy12345.us-east-1
      user: <your-username>
      password: <your-password>
      role: <your-role>                # e.g. SYSADMIN
      warehouse: <your-warehouse>      # e.g. COMPUTE_WH
      database: WBMIQA_ORCHPOC_DB
      schema: PUBLIC
      threads: 4
```

#### SSO / External Browser

```yaml
snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-account-id>
      user: <your-email>
      authenticator: externalbrowser
      role: <your-role>
      warehouse: <your-warehouse>
      database: WBMIQA_ORCHPOC_DB
      schema: PUBLIC
      threads: 4
```

### 2. Install dbt packages

```bash
dbt deps --profiles-dir .
```

This installs `dbt-utils` (used for surrogate key generation and deduplication).

### 3. Test the connection

```bash
dbt debug --profiles-dir .
```

You should see `All checks passed!` if everything is configured correctly.

---

## Running the Project

All commands should be run from the `snowflake/` directory. You **must** provide the `env` variable to specify the target environment.

### First-Time Build

```bash
# Install dependencies
dbt deps --profiles-dir .

# Load seed reference data, build all models, run tests
dbt build --vars '{env: KENT}' --profiles-dir .
```

### Subsequent Runs (after new monthly data arrives)

```bash
# Incremental build — only processes new data
dbt build --vars '{env: KENT}' --profiles-dir .
```

### Step-by-Step (if you prefer)

```bash
dbt seed  --vars '{env: KENT}' --profiles-dir .    # Load seed CSVs
dbt run   --vars '{env: KENT}' --profiles-dir .    # Build models
dbt test  --vars '{env: KENT}' --profiles-dir .    # Run tests
```

### Useful Commands

```bash
# Run only staging models
dbt run --select staging --vars '{env: KENT}' --profiles-dir .

# Run only curated models (dim + fact)
dbt run --select curated --vars '{env: KENT}' --profiles-dir .

# Run only MRT aggregate models
dbt run --select mrt --vars '{env: KENT}' --profiles-dir .

# Run a specific model and all its upstream dependencies
dbt run --select +fact_yellow_taxi_trips --vars '{env: KENT}' --profiles-dir .

# Full refresh — rebuild all incremental tables from scratch
dbt build --full-refresh --vars '{env: KENT}' --profiles-dir .

# Generate and serve documentation
dbt docs generate --vars '{env: KENT}' --profiles-dir .
dbt docs serve --profiles-dir .
```

### Running Against a Different Environment

Just change the `env` value — the source, curated, and MRT schemas all follow:

```bash
# QA environment
dbt build --vars '{env: QA}' --profiles-dir .
# Reads from QA_EDL, writes to QA_CURATED + QA_MRT

# Production
dbt build --vars '{env: PROD}' --profiles-dir .
# Reads from PROD_EDL, writes to PROD_CURATED + PROD_MRT
```

---

## Project Structure

```
snowflake/
├── dbt_project.yml              # Project config (profile, materialization, schema routing)
├── packages.yml                 # dbt package dependencies (dbt-utils)
├── profiles.yml                 # Local credentials (git-ignored)
├── .gitignore
├── macros/
│   └── generate_schema_name.sql # Custom macro: combines env var + schema label
├── seeds/
│   ├── vendor.csv
│   ├── rate_code.csv
│   ├── payment_type.csv
│   └── store_and_fwd_flag.csv
└── models/
    ├── sources.yml              # Source definitions ({env}_EDL tables)
    ├── staging/                 # → {env}_CURATED (views)
    │   ├── _staging__models.yml
    │   ├── stg_yellow_taxi_trips.sql
    │   └── stg_taxi_zone_lookup.sql
    ├── curated/                 # → {env}_CURATED (tables)
    │   ├── _curated__models.yml
    │   ├── dim_location.sql
    │   └── fact_yellow_taxi_trips.sql
    └── mrt/                     # → {env}_MRT (tables)
        ├── _mrt__models.yml
        ├── agg_daily_zone_summary.sql
        ├── agg_monthly_zone_summary.sql
        ├── agg_hourly_demand.sql
        └── agg_vendor_performance.sql
```
