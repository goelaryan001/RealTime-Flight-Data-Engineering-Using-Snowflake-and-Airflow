"""
Run with: pytest tests/test_cleanup.py -v
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.cleanup import find_stale_files


def _touch(path: Path, age_days: float, now: datetime):
    path.write_text("{}")
    mtime = (now - timedelta(days=age_days)).timestamp()
    os.utime(path, (mtime, mtime))


def test_flags_files_older_than_retention(tmp_path):
    now = datetime(2026, 1, 10)
    old_file = tmp_path / "flights_20260101000000.json"
    _touch(old_file, age_days=5, now=now)

    stale = find_stale_files(tmp_path, retention_days=2, now=now)
    assert stale == [old_file]


def test_keeps_files_within_retention(tmp_path):
    now = datetime(2026, 1, 10)
    recent_file = tmp_path / "flights_20260109120000.json"
    _touch(recent_file, age_days=0.5, now=now)

    stale = find_stale_files(tmp_path, retention_days=2, now=now)
    assert stale == []


def test_ignores_non_matching_files(tmp_path):
    now = datetime(2026, 1, 10)
    other_file = tmp_path / "notes.txt"
    _touch(other_file, age_days=10, now=now)

    stale = find_stale_files(tmp_path, retention_days=2, now=now)
    assert stale == []


def test_missing_directory_returns_empty_list(tmp_path):
    missing_dir = tmp_path / "does_not_exist"
    assert find_stale_files(missing_dir, retention_days=2, now=datetime(2026, 1, 10)) == []
