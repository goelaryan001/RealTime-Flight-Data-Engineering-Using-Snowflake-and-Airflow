import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")
if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_layer import run_bronze_ingestion
from scripts.silver_layer import run_silver_transform
from scripts.data_quality import run_data_quality_task
from scripts.gold_layer import run_gold_layer
from scripts.snowflake_implement import snowflake_load
from scripts.alerts import slack_failure_alert

default_args = {
    "owner": "airflow",
    "retries": 3,                              # was 0 — retry_delay below was previously dead config
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": slack_failure_alert,  # every task alerts on failure, not just the DAG as a whole
}

with DAG(
    dag_id="flights_ops_medallion_pipe",
    default_args=default_args,
    description="Polls OpenSky every 30 min, runs it through a bronze/silver/gold medallion pipeline, loads to Snowflake",
    start_date=datetime(2025, 12, 10),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["flights", "medallion", "snowflake"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
    )

    quality_gate = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_task,
    )

    gold = PythonOperator(
        task_id="gold_layer",
        python_callable=run_gold_layer,
    )

    snowflake = PythonOperator(
        task_id="snowflake_load",
        python_callable=snowflake_load,
    )

    # quality_gate sits between silver and gold deliberately — bad data gets
    # caught and alerted on before it reaches aggregation or the warehouse,
    # not after
    bronze >> silver >> quality_gate >> gold >> snowflake
