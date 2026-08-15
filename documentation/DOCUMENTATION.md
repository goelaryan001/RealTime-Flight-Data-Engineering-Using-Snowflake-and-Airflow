# Flight Pipeline — Documentation

This document covers the **v2 rebuild** (`flight_pipeline_v2/`), the only version of this pipeline that's built and run going forward.

For step-by-step run instructions (docker compose, tests, project structure) see [`flight_pipeline_v2/README.md`](../flight_pipeline_v2/README.md). This file covers environment setup, the DAG, the Snowflake schema, and dashboard queries.

## ENVIRONMENT VARIABLES

`flight_pipeline_v2/.env` is **gitignored** and must never be committed — it holds your own local Postgres/Airflow-admin credentials, generated per-environment, not shared or reused from the original tutorial.

Setup:
```bash
cd flight_pipeline_v2
cp .env.example .env
# edit .env and fill in your own values
```

Template (`.env.example`):
```yaml
# ---------- POSTGRES (Airflow's metadata DB, local to docker-compose only) ----------
POSTGRES_USER=airflow
POSTGRES_PASSWORD=changeme
POSTGRES_DB=airflow

# ---------- AIRFLOW ADMIN (login for the localhost:8080 webserver UI) ----------
AIRFLOW_ADMIN_USER=changeme
AIRFLOW_ADMIN_FIRSTNAME=changeme
AIRFLOW_ADMIN_LASTNAME=changeme
AIRFLOW_ADMIN_EMAIL=changeme@example.com
AIRFLOW_ADMIN_PASSWORD=changeme
```

Snowflake credentials are **not** stored in `.env` — they're set up as an Airflow Connection through the webserver UI (`Admin > Connections`, id `flight_snowflake`) after `docker compose up`, so the password only ever lives encrypted in Airflow's metadata DB, never in a file on disk. See the README's "Running it" section for the exact fields.

Slack alerting is optional: set the `SLACK_WEBHOOK_URL` Airflow Variable (`Admin > Variables`) to enable `on_failure_callback` notifications. If unset, task failures are logged but not posted anywhere — the pipeline runs fine without it.

## DATA SOURCE

```yaml
https://opensky-network.org/api/states/all
```
Docs: `https://openskynetwork.github.io/opensky-api/rest.html`

## PIPELINE ARCHITECTURE

```
OpenSky API → [Bronze: raw JSON] → [Silver: cleaned, validated, typed]
   → [Data Quality Gate] → [Gold: time-windowed country aggregates]
   → Snowflake STAGING (MERGE upsert) → Snowflake ANALYTICS (views)
```

Five Airflow tasks, in order: `bronze_ingest >> silver_transform >> data_quality_check >> gold_layer >> snowflake_load`.

- **Bronze** — pulls `/states/all` and writes the raw JSON untouched. No validation by design, so silver can be re-run against a fixed snapshot while debugging without re-hitting a live, constantly-changing API.
- **Silver** — dedups on `icao24`, drops fully-null aircraft, drops physically implausible velocity (> 350 m/s) and stale position reports (> 900s gap between `time_position` and `last_contact`), casts explicit dtypes, and retains `time_position`/`latitude`/`longitude` (the original tutorial dropped these).
- **Data quality gate** — asserts row count, required columns, `icao24` uniqueness/non-null, non-negative velocity, non-null `origin_country`. Sits between silver and gold specifically so bad data never reaches aggregation or Snowflake, and a failure here is a specific, actionable Slack message rather than a generic task failure.
- **Gold** — aggregates into 5-minute `(window_start, origin_country)` buckets using `time_position`, rather than assuming the whole file represents one point in time.
- **Snowflake load** — `MERGE`s gold's output into `STAGING`, idempotent on `(window_start, origin_country)`.

DAG-level retry policy (`dags/flight_pipeline.py`):
```python
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": slack_failure_alert,
}
```
Applied via `default_args`, so every task in the DAG gets retries, backoff, and failure alerting for free — a task added later inherits this without extra wiring.

## SNOWFLAKE SCHEMA

Full script: [`flight_pipeline_v2/scripts/snowflake_schema.sql`](../flight_pipeline_v2/scripts/snowflake_schema.sql). Run it once, in a Snowflake worksheet, before the first DAG run.

```sql
CREATE DATABASE IF NOT EXISTS FLIGHTS;
CREATE SCHEMA IF NOT EXISTS FLIGHTS.STAGING;
CREATE SCHEMA IF NOT EXISTS FLIGHTS.ANALYTICS;

-- One row per (window_start, origin_country), loaded directly by the pipeline
CREATE TABLE IF NOT EXISTS FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS (
    window_start      TIMESTAMP_NTZ,
    origin_country     TEXT,
    total_flights       INT,
    avg_velocity        FLOAT,
    max_velocity        FLOAT,
    avg_altitude         FLOAT,
    on_ground_count     INT,
    load_time            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, origin_country)
);
```

`STAGING` holds data close to what the pipeline lands. `ANALYTICS` holds two views built on top of it — a dashboard or BI tool should query `ANALYTICS`, never `STAGING` directly:

- **`COUNTRY_DAILY_SUMMARY`** — daily rollup per country with a `RANK() OVER (...)` window function, so "top N countries" is `WHERE rank_by_volume <= 5`, not re-derived per dashboard query.
- **`ROLLING_24H_ACTIVITY`** — trailing 24h activity per country; `MAX(load_time)` doubles as a pipeline freshness check.

## DASHBOARD / ANALYSIS QUERIES

Query `ANALYTICS`, not `STAGING`, for all of these.

### 1. Top 5 countries today
```sql
SELECT origin_country, total_flights, rank_by_volume
FROM FLIGHTS.ANALYTICS.COUNTRY_DAILY_SUMMARY
WHERE flight_date = CURRENT_DATE()
ORDER BY rank_by_volume
LIMIT 5;
```

### 2. Countries with the fastest average speed today
```sql
SELECT origin_country, avg_velocity
FROM FLIGHTS.ANALYTICS.COUNTRY_DAILY_SUMMARY
WHERE flight_date = CURRENT_DATE()
ORDER BY avg_velocity DESC
LIMIT 5;
```

### 3. Total flights, last 24 hours
```sql
SELECT SUM(flights_last_24h) AS total_flights
FROM FLIGHTS.ANALYTICS.ROLLING_24H_ACTIVITY;
```

### 4. Active countries, last 24 hours
```sql
SELECT COUNT(DISTINCT origin_country) AS countries
FROM FLIGHTS.ANALYTICS.ROLLING_24H_ACTIVITY;
```

### 5. Flight activity trend over time
```sql
SELECT flight_date, SUM(total_flights) AS total_flights
FROM FLIGHTS.ANALYTICS.COUNTRY_DAILY_SUMMARY
GROUP BY flight_date
ORDER BY flight_date;
```

### 6. Pipeline freshness check
```sql
SELECT MAX(last_loaded_at) AS last_loaded_at
FROM FLIGHTS.ANALYTICS.ROLLING_24H_ACTIVITY;
```
If this timestamp is more than ~30-40 minutes old, the DAG (scheduled every 30 min) has stopped landing data — check the Airflow UI.
