"""
Bronze layer: pulls the raw OpenSky /states/all snapshot and writes it
to disk untouched. Bronze is intentionally "dumb" — no cleaning or
validation here, so a bad API response is always recoverable by
re-running silver against the raw file, rather than being lost.
"""
import json
import logging
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

URL = "https://opensky-network.org/api/states/all"
REQUEST_TIMEOUT_SEC = 30


def run_bronze_ingestion(**context):
    logger.info(f"Requesting {URL}")
    response = requests.get(URL, timeout=REQUEST_TIMEOUT_SEC)
    response.raise_for_status()

    data = response.json()
    state_count = len(data.get("states") or [])
    logger.info(f"Received {state_count} state vectors")

    if state_count == 0:
        logger.warning("OpenSky returned zero state vectors — API may be rate-limiting or degraded")

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f)

    logger.info(f"Wrote bronze file: {path}")
    context["ti"].xcom_push(key="bronze_file", value=str(path))
