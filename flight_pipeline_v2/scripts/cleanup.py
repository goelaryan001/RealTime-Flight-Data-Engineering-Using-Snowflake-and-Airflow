"""
Retention cleanup for the bronze layer.

Every 30-minute poll writes a new timestamped bronze file
(flights_<timestamp>.json) and never overwrites one — that's deliberate, so a
bad API response is always recoverable by re-running silver against the raw
file. Silver and gold don't have this problem since their filenames are
scoped to ds_nodash and get overwritten within the same day. Left alone,
bronze would grow unbounded (48 files/day), so this deletes files older than
RETENTION_DAYS on every run, before the next bronze file is written.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

RETENTION_DAYS = 2


def find_stale_files(directory: Path, retention_days: int, now: datetime) -> list:
    """Pure function: directory + retention window + reference time -> stale files."""
    if not directory.exists():
        return []

    cutoff = now - timedelta(days=retention_days)
    return [
        f for f in directory.glob("flights_*.json")
        if datetime.utcfromtimestamp(f.stat().st_mtime) < cutoff
    ]


def run_bronze_cleanup(**context):
    """Airflow task wrapper — deletes bronze files older than RETENTION_DAYS."""
    directory = Path("/opt/airflow/data/bronze")
    stale = find_stale_files(directory, RETENTION_DAYS, datetime.utcnow())

    for f in stale:
        f.unlink()

    logger.info(f"Bronze cleanup: removed {len(stale)} file(s) older than {RETENTION_DAYS}d")
