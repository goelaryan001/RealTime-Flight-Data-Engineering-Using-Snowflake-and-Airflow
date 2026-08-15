# Real-Time Flight Data Pipeline — Airflow + Snowflake

Polls the [OpenSky Network](https://opensky-network.org/) live flight-state API every 30 minutes, runs the data through a bronze/silver/gold medallion pipeline with an explicit data quality gate, and loads curated aggregates into Snowflake through a staging → analytics schema.

This started from a public tutorial project and was substantially rebuilt: real data cleaning and validation, a data quality gate with alerting, genuine time-windowed aggregation, a layered Snowflake schema instead of one flat table, and a tested transform layer.

## Architecture

```
OpenSky API → [Bronze: raw JSON] → [Silver: cleaned, validated, typed]
   → [Data Quality Gate] → [Gold: time-windowed country aggregates]
   → Snowflake STAGING (MERGE upsert) → Snowflake ANALYTICS (views)
   → [Dashboard: reads ANALYTICS back out, writes data/dashboard/latest.html]
```

Each stage writes its own dated file (`data/bronze/`, `data/silver/`, `data/gold/`) rather than a single job doing everything in memory — this makes it possible to re-run any single stage against the same input, which matters a lot when debugging why gold looks wrong: you can inspect exactly what silver produced without re-hitting the live API.

## Design decisions and trade-offs

**Why "near-real-time" polling instead of true streaming.** OpenSky's REST API is a snapshot endpoint, not a stream — there's no webhook or Kafka topic to subscribe to. A 30-minute poll is a reasonable trade-off for this data: individual flight positions change constantly, but country-level traffic patterns (the actual gold-layer output) don't meaningfully shift minute to minute. A true streaming architecture (Kafka + Spark Structured Streaming) would be the right upgrade if the goal were per-flight tracking rather than aggregate country statistics — worth naming explicitly as the next evolution rather than pretending this already is one.

**Why bronze stays "dumb."** Bronze does zero validation — it writes whatever the API returns, even if that's malformed or empty. This is deliberate: if silver's cleaning logic has a bug, I can fix it and re-run silver against the exact same bronze file, instead of needing to re-hit a live API that returns different data every time I retry. Cleaning happens exactly once, in exactly one place (silver).

**Why the data quality gate is a separate DAG task, not folded into silver.** Putting quality checks in their own task means a quality failure shows up as its own red task in the Airflow UI and triggers its own Slack alert with a specific assertion message — "duplicate icao24 survived dedup" is a much faster thing to debug at 2am than "the pipeline failed somewhere in silver_transform." It also means gold and Snowflake genuinely never run against data that failed validation, rather than relying on hoping silver's logic was airtight.

**Why velocity/staleness thresholds are hard-coded constants, not configurable.** For a project at this scale, YAGNI — the values (350 m/s max velocity, 900s staleness) are commercial-aviation domain constants, not something that needs to vary by environment. If this were multi-tenant or needed to handle military/experimental aircraft with different physical envelopes, they'd move to Airflow Variables instead.

**Why the Snowflake staging/analytics split matters.** The original version had one flat table that pipeline code, ad-hoc queries, and dashboards would all hit directly — meaning any dashboard query author needed to know the raw upsert grain (window × country) and re-derive rollups themselves every time. Splitting into `STAGING.STG_FLIGHT_WINDOW_METRICS` (what the pipeline writes) and `ANALYTICS.COUNTRY_DAILY_SUMMARY` / `ANALYTICS.ROLLING_24H_ACTIVITY` (views a dashboard actually queries) means the aggregation logic for "top countries today" lives in exactly one place, not copy-pasted across every dashboard tile.

**Why plain `assert` statements instead of Great Expectations for data quality.** At this scale (5 checks, 1 table), a real Great Expectations suite would be more tooling overhead than the problem justifies. The checks are structured so swapping in a real GX checkpoint later wouldn't require changing the DAG's shape — just what's inside `run_data_quality_task`.

## What I'd do differently at 10x scale

- Move to actual streaming ingestion (Kafka) if per-flight tracking mattered, not just country aggregates
- Replace the hand-rolled MERGE loop (one `cursor.execute` per row) with a bulk `COPY INTO` staged load — row-by-row MERGE doesn't scale past a few thousand rows per run
- Move the ANALYTICS views to dbt models, both for testability and to get lineage documentation for free
- Add a proper Great Expectations suite once there's enough tables/columns that hand-written assertions stop being the fastest option

## Running it

```bash
cp .env.example .env   # fill in your own credentials, do not commit .env
docker compose up -d
# Airflow UI at localhost:8080
# Add a Snowflake connection (Admin > Connections) named `flight_snowflake`
# Add an Airflow Variable named SLACK_WEBHOOK_URL for failure alerts
```

Run the schema setup once before the first DAG run:
```bash
snowsql -f scripts/snowflake_schema.sql
```

Run tests:
```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Project structure

```
dags/flight_pipeline.py       # DAG definition and task wiring
scripts/bronze_layer.py       # raw API ingestion
scripts/silver_layer.py       # cleaning, validation, typing (pure function + Airflow wrapper)
scripts/data_quality.py       # quality gate (pure function + Airflow wrapper)
scripts/gold_layer.py         # time-windowed aggregation (pure function + Airflow wrapper)
scripts/snowflake_implement.py # Snowflake load
scripts/snowflake_schema.sql  # staging + analytics schema definition
scripts/dashboard.py          # reads ANALYTICS views back out, renders data/dashboard/latest.html
scripts/alerts.py             # Slack failure callback
tests/                        # pytest suite + mock API response for offline testing
```
