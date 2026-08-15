"""
Data quality gate — runs between silver and gold. Fails the DAG loudly
(triggering the Slack alert) if the cleaned data doesn't meet baseline
expectations, rather than letting bad data flow silently into gold and
Snowflake.

This is intentionally simple (plain assertions, not a Great Expectations
suite) — the point for this project's scale is correctness and visibility,
not tooling sophistication. It's straightforward to swap in a real
Great Expectations checkpoint here later without changing the DAG's shape.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

MIN_EXPECTED_ROWS = 1
REQUIRED_COLUMNS = ["icao24", "origin_country", "velocity", "on_ground", "time_position"]


def run_quality_checks(df: pd.DataFrame) -> None:
    """Raises AssertionError on the first failed check, with a message
    specific enough to debug from the Slack alert alone."""

    assert len(df) >= MIN_EXPECTED_ROWS, (
        f"Silver output has {len(df)} rows, expected at least {MIN_EXPECTED_ROWS} — "
        f"likely an upstream API or filtering problem"
    )

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    assert not missing_cols, f"Silver output is missing required columns: {missing_cols}"

    assert df["icao24"].is_unique, "Duplicate icao24 values survived the silver dedup step"

    null_icao = df["icao24"].isna().sum()
    assert null_icao == 0, f"{null_icao} rows have a null icao24 (primary identifier) — should be impossible"

    bad_velocity = df["velocity"].dropna()
    assert (bad_velocity >= 0).all(), "Found negative velocity values — sensor data corruption"

    bad_country = df["origin_country"].isna().sum()
    assert bad_country == 0, f"{bad_country} rows have a null origin_country"

    logger.info(f"Data quality checks passed: {len(df)} rows, {df['origin_country'].nunique()} countries")


def run_data_quality_task(**context):
    """Airflow task wrapper."""
    silver_file = context["ti"].xcom_pull(key="silver_file", task_ids="silver_transform")
    if not silver_file:
        raise ValueError("Silver file not found in XCom")

    df = pd.read_csv(silver_file)
    run_quality_checks(df)
