# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project layout

`flight_pipeline_v2/` is the only maintained version of this project — an Airflow DAG that polls the OpenSky Network's live flight-state API every 30 minutes, runs it through a bronze/silver/gold medallion pipeline with an explicit data quality gate, and loads curated aggregates into Snowflake through a staging → analytics schema. `documentation/DOCUMENTATION.md` and `architechture/` hold top-level architecture docs/diagram; `flight_pipeline_v2/README.md` has the detailed run guide and design-decision rationale. All commands below assume `cd flight_pipeline_v2` first.

## Commands

Run tests (no Airflow install required — see architecture note below):
```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```
Single test file or test:
```bash
pytest tests/test_silver_layer.py -v
pytest tests/test_silver_layer.py::test_drops_stale_reports -v
```

Local stack:
```bash
cp .env.example .env   # fill in your own values; .env is gitignored
docker compose up -d
# Airflow UI at localhost:8080
```
Set up Snowflake once (run `scripts/snowflake_schema.sql` in a Snowflake worksheet), then in the Airflow UI: add a Connection `flight_snowflake` (extra JSON: `{"account", "warehouse", "role"}`), and optionally an Airflow Variable `SLACK_WEBHOOK_URL` for failure alerts.

Trigger a DAG run from the CLI (inside the webserver container):
```bash
docker exec airflow-webserver airflow dags unpause flights_ops_medallion_pipe
docker exec airflow-webserver airflow dags trigger flights_ops_medallion_pipe
docker exec airflow-webserver airflow tasks states-for-dag-run flights_ops_medallion_pipe <run_id>
```

## Architecture

**Task graph:** `bronze_ingest >> silver_transform >> data_quality_check >> gold_layer >> snowflake_load >> generate_dashboard`, defined in `dags/flight_pipeline.py`. Each stage is a `PythonOperator` calling into `scripts/<stage>.py`; file paths are passed between tasks via XCom, not the data itself.

**Pure-function-plus-thin-wrapper pattern, applied consistently:** every script in `scripts/` (except `bronze_layer.py`, which has no meaningful pure logic) splits into a pure transform function (dict/DataFrame in, DataFrame/int out, zero Airflow dependency) and a `run_*`/`snowflake_load` wrapper that only handles XCom I/O and Airflow-specific imports. The Airflow import itself (`from airflow...`) is deliberately deferred *inside* the wrapper function, never at module level — this is what lets `tests/` run against pure pandas logic with only `pandas`/`pytest` (and `snowflake-connector-python` for `snowflake_implement.py`, a real dependency) installed, no Airflow required at all. When adding a new script, preserve this split; putting an Airflow import at module level silently breaks that testability.

**Data flow across stages, each writing its own dated file rather than one in-memory job:**
- `bronze_layer.py` → raw OpenSky JSON, `data/bronze/flights_<timestamp>.json`, zero validation by design (re-run silver against a bad file without re-hitting a live API)
- `silver_layer.py` → `clean_flight_data(raw_dict) -> DataFrame`: dedups on `icao24`, drops fully-null aircraft, drops implausible velocity (> `MAX_PLAUSIBLE_VELOCITY_MS`) and stale reports (`time_position` vs `last_contact` gap > `STALE_REPORT_THRESHOLD_SEC`), casts explicit dtypes, retains lat/long/altitude/time_position. Writes `data/silver/flights_silver_<ds_nodash>.csv`.
- `data_quality.py` → `run_quality_checks(df)`: plain `assert` statements (row count, required columns, `icao24` uniqueness/non-null, non-negative velocity, non-null country). Runs as its own task between silver and gold so a failure is a specific, actionable Slack message and gold/Snowflake never see unvalidated data.
- `gold_layer.py` → `build_gold_aggregates(df) -> DataFrame`: buckets by `(window_start, origin_country)` where `window_start` is `time_position` floored to `WINDOW_MINUTES` (5) — not the whole file grouped as one window. Writes `data/gold/flights_gold_<ds_nodash>.csv`.
- `snowflake_implement.py` → `load_to_snowflake(df, connection_params) -> int`: row-by-row `MERGE` into `FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS`, idempotent on `(window_start, origin_country)`. `_nullable_float()` converts NaN to `None` before binding — Snowflake's connector renders `float('nan')` as the bare SQL token `NAN`, which fails as an invalid identifier rather than binding as NULL; this only surfaces against live data where an aggregate group has all-null source values, not the mock fixture.

**Snowflake schema** (`scripts/snowflake_schema.sql`): `STAGING.STG_FLIGHT_WINDOW_METRICS` is what the pipeline writes; `ANALYTICS.COUNTRY_DAILY_SUMMARY` and `ANALYTICS.ROLLING_24H_ACTIVITY` are views built on top — queries and dashboards should hit `ANALYTICS`, never `STAGING` directly.

**Dashboard** (`scripts/dashboard.py`): the last DAG task, running only after `snowflake_load` succeeds — queries both `ANALYTICS` views and renders a self-contained HTML report (`render_dashboard_html`, pure function, no charting library — inline SVG bar charts built from plain geometry) to `data/dashboard/latest.html`. Regenerated every run, so it always reflects what's actually in Snowflake, not a stale snapshot.

**Alerting/reliability:** `alerts.py`'s `slack_failure_alert` is wired into `default_args["on_failure_callback"]` in the DAG (applies to every task, not attached per-task), reading the webhook URL from the `SLACK_WEBHOOK_URL` Airflow Variable — logs a warning rather than silently no-op'ing if unset, and swallows its own POST failures so a broken alert channel never masks the real task failure. `default_args` also sets `retries=3` with `retry_exponential_backoff=True` and a 30-minute cap.

## Testing

`tests/mock_opensky_response.json` is a hand-built fixture with one row per edge case the silver transform needs to handle (an exact-duplicate `icao24`, a fully-null aircraft, an implausible-velocity row, a stale-report row) — when adding a new cleaning rule to `silver_layer.py`, add a corresponding row to this fixture rather than relying only on synthetic DataFrames in the test file itself, so `test_data_quality.py` and `test_gold_layer.py` (which both build off `clean_flight_data()` against the same fixture) stay consistent.
