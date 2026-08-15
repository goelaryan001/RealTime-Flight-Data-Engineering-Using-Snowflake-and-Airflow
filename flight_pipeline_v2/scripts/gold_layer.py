"""
Gold layer: cleaned silver DataFrame -> country-level aggregates, bucketed
into 5-minute time windows using the actual flight timestamp.

The previous version aggregated the entire silver file as one flat group,
which only worked "by accident" because each DAG run's silver file already
corresponded to a single poll. Since silver now retains time_position, this
version buckets explicitly — the aggregation logic no longer depends on an
unstated assumption about how the file happens to be scoped upstream, which
makes it correct even if silver's granularity changes later (e.g. if bronze
starts polling more frequently, or backfills multiple polls into one file).
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

WINDOW_MINUTES = 5


def build_gold_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Pure transform: cleaned silver DataFrame -> windowed country aggregates."""
    if df.empty:
        raise ValueError("Silver DataFrame is empty — nothing to aggregate")

    working = df.copy()
    working["window_start"] = (
        pd.to_datetime(working["time_position"], unit="s")
        .dt.floor(f"{WINDOW_MINUTES}min")
    )

    agg = (
        working.groupby(["window_start", "origin_country"])
        .agg(
            total_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean"),
            max_velocity=("velocity", "max"),
            avg_altitude=("baro_altitude", "mean"),
            on_ground_count=("on_ground", "sum"),
        )
        .reset_index()
    )

    agg["avg_velocity"] = agg["avg_velocity"].round(2)
    agg["avg_altitude"] = agg["avg_altitude"].round(1)

    logger.info(f"Gold aggregation: {len(df)} flight records -> {len(agg)} (window, country) rows")
    return agg


def run_gold_layer(**context):
    """Airflow task wrapper."""
    from pathlib import Path

    silver_file = context["ti"].xcom_pull(key="silver_file", task_ids="silver_transform")
    if not silver_file:
        raise ValueError("Silver file not found in XCom")

    df = pd.read_csv(silver_file)
    agg = build_gold_aggregates(df)

    gold_path = Path(silver_file.replace("silver", "gold"))
    gold_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(gold_path, index=False)

    context["ti"].xcom_push(key="gold_file", value=str(gold_path))
