"""
Loads gold-layer aggregates into FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS
via MERGE (idempotent upsert on window_start + origin_country — safe to
re-run without creating duplicate rows).

ANALYTICS.COUNTRY_DAILY_SUMMARY and ANALYTICS.ROLLING_24H_ACTIVITY are
Snowflake views defined in snowflake_schema.sql, built on top of this
staging table — dashboards should query those, not this table directly.
"""
import logging
import pandas as pd
import snowflake.connector

logger = logging.getLogger(__name__)

MERGE_SQL = """
    MERGE INTO FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS tgt
    USING (
        SELECT
            TO_TIMESTAMP(%s) AS WINDOW_START,
            %s AS ORIGIN_COUNTRY,
            %s AS TOTAL_FLIGHTS,
            %s AS AVG_VELOCITY,
            %s AS MAX_VELOCITY,
            %s AS AVG_ALTITUDE,
            %s AS ON_GROUND_COUNT
    ) src
    ON tgt.WINDOW_START = src.WINDOW_START
       AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY
    WHEN MATCHED THEN UPDATE SET
        TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
        AVG_VELOCITY = src.AVG_VELOCITY,
        MAX_VELOCITY = src.MAX_VELOCITY,
        AVG_ALTITUDE = src.AVG_ALTITUDE,
        ON_GROUND_COUNT = src.ON_GROUND_COUNT,
        LOAD_TIME = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, MAX_VELOCITY, AVG_ALTITUDE, ON_GROUND_COUNT)
    VALUES
        (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.MAX_VELOCITY, src.AVG_ALTITUDE, src.ON_GROUND_COUNT);
"""


def _nullable_float(value):
    """NaN (e.g. avg_altitude for a window/country where every aircraft had
    a null baro_altitude) must become SQL NULL, not float('nan') — the
    Snowflake connector's parameter binder renders float('nan') as the bare
    token NAN, which Snowflake parses as an invalid identifier rather than
    a float literal, and the MERGE fails with a SQL compilation error."""
    return None if pd.isna(value) else float(value)


def load_to_snowflake(df: pd.DataFrame, connection_params: dict) -> int:
    """Pure-ish function: takes a gold DataFrame and open connection params,
    returns the number of rows merged. Connection is created here rather than
    passed in so this stays a single unit to call from the Airflow task,
    but the SQL/row-building logic is isolated from XCom/Airflow specifics
    for easier testing with a mocked connection."""
    conn = snowflake.connector.connect(**connection_params)
    rows_merged = 0
    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                cursor.execute(
                    MERGE_SQL,
                    (
                        int(row["window_start"]) if isinstance(row["window_start"], (int, float)) else row["window_start"],
                        row["origin_country"],
                        int(row["total_flights"]),
                        _nullable_float(row["avg_velocity"]),
                        _nullable_float(row["max_velocity"]),
                        _nullable_float(row["avg_altitude"]),
                        int(row["on_ground_count"]),
                    ),
                )
                rows_merged += 1
    finally:
        conn.close()
    logger.info(f"Merged {rows_merged} rows into FLIGHTS.STAGING.STG_FLIGHT_WINDOW_METRICS")
    return rows_merged


def snowflake_load(**context):
    """Airflow task wrapper."""
    from airflow.hooks.base import BaseHook

    gold_file = context["ti"].xcom_pull(key="gold_file", task_ids="gold_layer")
    if not gold_file:
        raise ValueError("Gold file not found in XCom")

    df = pd.read_csv(gold_file)

    conn = BaseHook.get_connection("flight_snowflake")
    connection_params = dict(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database="FLIGHTS",
        schema="STAGING",
        role=conn.extra_dejson.get("role"),
    )

    load_to_snowflake(df, connection_params)
