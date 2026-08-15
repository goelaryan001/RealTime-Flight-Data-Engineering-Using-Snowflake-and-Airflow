-- ============================================================
-- Flight pipeline Snowflake schema — staging + analytics layers
-- ============================================================
-- Previous version had a single flat table with no layering at all.
-- This mirrors the medallion pattern already used upstream: STAGING
-- holds data close to what the pipeline lands (one row per window,
-- per country), ANALYTICS holds derived, business-facing views built
-- on top of staging. A dashboard or BI tool should query ANALYTICS,
-- never STAGING directly.

CREATE DATABASE IF NOT EXISTS FLIGHTS;

CREATE SCHEMA IF NOT EXISTS FLIGHTS.STAGING;
CREATE SCHEMA IF NOT EXISTS FLIGHTS.ANALYTICS;

-- ---------- STAGING ----------
-- One row per (time window, country), loaded directly by the pipeline
-- via MERGE (idempotent upsert — safe to re-run a DAG task without
-- creating duplicates).
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

-- ---------- ANALYTICS ----------
-- Business-facing view: daily rollup per country, plus a rank so
-- "top N countries" queries don't need to recompute aggregation logic
-- in every dashboard query. This is what a BI tool / Snowflake
-- dashboard should actually point at.
CREATE OR REPLACE VIEW FLIGHTS.ANALYTICS.COUNTRY_DAILY_SUMMARY AS
SELECT
    DATE(window_start)                         AS flight_date,
    origin_country,
    SUM(total_flights)                          AS total_flights,
    ROUND(AVG(avg_velocity), 2)                AS avg_velocity,
    MAX(max_velocity)                            AS peak_velocity,
    ROUND(AVG(avg_altitude), 1)                AS avg_altitude,
    SUM(on_ground_count)                        AS total_on_ground,
    RANK() OVER (
        PARTITION BY DATE(window_start)
        ORDER BY SUM(total_flights) DESC
    )                                            AS rank_by_volume
FROM FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS
GROUP BY DATE(window_start), origin_country;

-- Rolling 24-hour activity, for a "is the pipeline actually current"
-- freshness check as well as a live activity view
CREATE OR REPLACE VIEW FLIGHTS.ANALYTICS.ROLLING_24H_ACTIVITY AS
SELECT
    origin_country,
    SUM(total_flights)              AS flights_last_24h,
    ROUND(AVG(avg_velocity), 2)     AS avg_velocity_last_24h,
    MAX(load_time)                   AS last_loaded_at
FROM FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS
WHERE window_start >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY origin_country
ORDER BY flights_last_24h DESC;
