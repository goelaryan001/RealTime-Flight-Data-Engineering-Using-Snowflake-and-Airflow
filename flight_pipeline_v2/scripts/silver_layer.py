"""
Silver layer: raw bronze JSON -> cleaned, validated, typed DataFrame.

This is a genuine cleaning step, not just a column subset:
  - Deduplicates on icao24 (OpenSky can return overlapping state vectors
    across polls, and occasionally within a single poll)
  - Drops rows with no usable position/velocity data (fully-null aircraft
    that dropped off signal — not useful for any downstream analysis)
  - Flags and drops physically impossible velocity readings (bad sensor
    data — real aircraft don't exceed ~350 m/s in level flight)
  - Flags stale reports where time_position is far behind last_contact
    (indicates a delayed/unreliable position fix)
  - Casts every column to an explicit, correct dtype rather than trusting
    whatever pandas infers from a mixed-type JSON array
  - Retains latitude/longitude/altitude/time_position — the previous
    version silently dropped all of these, which also removed any ability
    to do genuine time-windowed or geospatial analysis downstream

The transform logic is a pure function (raw dict in, DataFrame out) with
no Airflow dependency, specifically so it's unit-testable in isolation.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source",
]

# Real-world sanity bounds. Commercial aircraft don't exceed these in
# normal operation; values outside this range indicate bad sensor data.
MAX_PLAUSIBLE_VELOCITY_MS = 350.0   # ~680 knots, above any commercial airliner's cruise speed
MAX_PLAUSIBLE_ALTITUDE_M = 15000.0  # ~49,000 ft, above typical commercial ceiling
STALE_REPORT_THRESHOLD_SEC = 900    # 15 min — a position fix older than this vs. last_contact is unreliable

KEEP_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
]


def clean_flight_data(raw: dict) -> pd.DataFrame:
    """Pure transform: raw OpenSky JSON payload -> cleaned DataFrame.

    Raises ValueError if the payload has no usable state vectors at all,
    so the caller (Airflow task) fails loudly instead of silently writing
    an empty file downstream.
    """
    states = raw.get("states")
    if not states:
        raise ValueError("No state vectors in bronze payload — API may have returned empty/error response")

    df = pd.DataFrame(states, columns=OPENSKY_COLUMNS)
    starting_rows = len(df)

    # 1. Drop exact duplicate icao24 entries, keeping the most recent by last_contact
    df = df.sort_values("last_contact", ascending=False).drop_duplicates(subset="icao24", keep="first")#We keep the newest one
    deduped_rows = len(df)

    # 2. Drop rows with no usable velocity AND no usable altitude — these are
    #    aircraft that dropped off signal; they carry no analytical value here
    df = df.dropna(subset=["velocity", "baro_altitude"], how="all")

    # 3. Explicit type casting — don't trust pandas' inferred dtypes from mixed JSON
    df["icao24"] = df["icao24"].astype(str)
    df["callsign"] = df["callsign"].astype(str).str.strip().replace({"None": None, "": None})
    df["origin_country"] = df["origin_country"].astype(str).str.strip().replace({"None": None, "": None})
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce")
    df["baro_altitude"] = pd.to_numeric(df["baro_altitude"], errors="coerce") #Coerce change values which cant be changed to Nan instead of crashing
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["on_ground"] = df["on_ground"].astype(bool)
    df["time_position"] = pd.to_numeric(df["time_position"], errors="coerce")
    df["last_contact"] = pd.to_numeric(df["last_contact"], errors="coerce")

    # 3b. Drop rows with no origin_country at all — gold's entire aggregation
    #     is grouped by country, so a row that can't be attributed to one has
    #     nowhere to go in the output and would otherwise resurface later as a
    #     confusing null-country failure in the data quality gate instead of
    #     being handled here, at the source.
    missing_country = df["origin_country"].isna()
    if missing_country.any():
        logger.warning(f"Dropping {missing_country.sum()} rows with no origin_country")
    df = df[~missing_country]

    # 4. Flag and drop physically impossible velocity readings (bad sensor data)
    implausible_velocity = df["velocity"] > MAX_PLAUSIBLE_VELOCITY_MS
    if implausible_velocity.any():
        logger.warning(f"Dropping {implausible_velocity.sum()} rows with implausible velocity (> {MAX_PLAUSIBLE_VELOCITY_MS} m/s)")
    df = df[~implausible_velocity.fillna(False)]

    # 5. Flag and drop stale position reports
    stale = (df["last_contact"] - df["time_position"]) > STALE_REPORT_THRESHOLD_SEC
    if stale.any():
        logger.warning(f"Dropping {stale.sum()} rows with stale position reports (> {STALE_REPORT_THRESHOLD_SEC}s old)")
    df = df[~stale.fillna(False)]

    df = df[KEEP_COLUMNS].reset_index(drop=True)

    logger.info(
        f"Silver transform: {starting_rows} raw -> {deduped_rows} after dedup -> {len(df)} after quality filtering"
    )
    return df


def run_silver_transform(**context):
    """Airflow task wrapper — pulls the bronze file path from XCom, runs the
    pure transform, writes the cleaned CSV, pushes the output path forward."""
    import json
    from pathlib import Path

    execution_date = context["ds_nodash"]
    bronze_file = context["ti"].xcom_pull(key="bronze_file", task_ids="bronze_ingest")
    if not bronze_file:
        raise ValueError("Bronze file path not found in XCom")

    with open(bronze_file) as f:
        raw = json.load(f)

    df = clean_flight_data(raw)

    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)
    output_file = silver_path / f"flights_silver_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    context["ti"].xcom_push(key="silver_file", value=str(output_file))
