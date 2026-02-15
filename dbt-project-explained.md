# dbt Project Structure -- taxi_rides_ny

## Project Overview

This project follows a standard analytics engineering architecture:

Python ingestion → DuckDB → dbt transformations → Analytics-ready tables
→ BI / ML

------------------------------------------------------------------------

## Folder Structure and Purpose
### analyses/

Ad-hoc analytical queries.

-   Used for exploration
-   Not materialized into tables
-   Data quality checks/reports that we don't want to share with stakesholders
-   Lot of people don't use it


------------------------------------------------------------------------

## Key Files

### dbt_project.yml

Project configuration file: the most important file in dbt as the dbt commands don't run without it.

-   Stores dbt defaults and variables
-   Defines project name
-   Sets profile name
-   Controls model behavior

------------------------------------------------------------------------

### taxi_rides_ny.duckdb

DuckDB database file.

-   Stores materialized tables

------------------------------------------------------------------------

### ingest.py

Python ingestion script.

-   Extracts raw data
-   Loads data into DuckDB
-   Prepares data for dbt transformations

------------------------------------------------------------------------


### macros/

Reusable SQL logic.

-   Similar to functions in Python
-   Avoids repeated SQL code

------------------------------------------------------------------------


### models/

Core of dbt.

-   Contains SQL transformation models
-   Each `.sql` file creates a table or view
-   Transforms raw data into staging and analytics tables

Typical flow: raw → staging → marts → BI / ML

------------------------------------------------------------------------

### seeds/

Static reference data.

-   Contains CSV files
-   dbt loads them as tables
-   Used for small lookup tables (zones, categories)

------------------------------------------------------------------------

### snapshots/

Historical tracking (Slowly Changing Dimensions).

-   Tracks changes over time
-   Captures historical versions of records

------------------------------------------------------------------------

### tests/

Data quality validation.

-   Ensures no nulls
-   Enforces unique keys
-   Validates business rules

------------------------------------------------------------------------




### data/

Optional folder for auxiliary or reference files.

------------------------------------------------------------------------

### dbt_packages/

Installed dbt packages.

-   Created after running `dbt deps`

------------------------------------------------------------------------

### logs/

Execution logs for debugging.

------------------------------------------------------------------------

### target/

Compiled artifacts.

-   Generated when running `dbt run`
-   Contains compiled SQL

------------------------------------------------------------------------



## Architecture Flow

Local Development:

Python → DuckDB → dbt → Analytics tables → BI / ML

Production Equivalent (GCP):

Python/Airflow → BigQuery → dbt-bigquery → Feature tables → Vertex AI
