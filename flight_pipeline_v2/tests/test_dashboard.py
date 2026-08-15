"""
Run with: pytest tests/test_dashboard.py -v
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.dashboard import render_dashboard_html


@pytest.fixture
def country_summary():
    return pd.DataFrame([
        {"flight_date": "2026-08-15", "origin_country": "United States", "total_flights": 5310,
         "avg_velocity": 92.89, "peak_velocity": 289.64, "avg_altitude": 3980.2,
         "total_on_ground": 496, "rank_by_volume": 1},
        {"flight_date": "2026-08-15", "origin_country": "Canada", "total_flights": 458,
         "avg_velocity": 71.78, "peak_velocity": 288.6, "avg_altitude": 6286.6,
         "total_on_ground": 67, "rank_by_volume": 2},
    ])


@pytest.fixture
def rolling_24h():
    return pd.DataFrame([
        {"origin_country": "United States", "flights_last_24h": 5310,
         "avg_velocity_last_24h": 92.89, "last_loaded_at": "2026-08-15 22:10:08"},
    ])


def test_renders_without_error(country_summary, rolling_24h):
    out = render_dashboard_html(country_summary, rolling_24h, datetime.now(timezone.utc))
    assert "<html" in out and "</html>" in out


def test_includes_country_names(country_summary, rolling_24h):
    out = render_dashboard_html(country_summary, rolling_24h, datetime.now(timezone.utc))
    assert "United States" in out
    assert "Canada" in out


def test_kpi_totals_are_correct(country_summary, rolling_24h):
    out = render_dashboard_html(country_summary, rolling_24h, datetime.now(timezone.utc))
    assert "5,768" in out  # 5310 + 458 total flights today


def test_handles_empty_data_without_crashing():
    empty = pd.DataFrame(columns=["origin_country", "total_flights", "avg_velocity"])
    out = render_dashboard_html(empty, empty, datetime.now(timezone.utc))
    assert "No data yet" in out


def test_escapes_country_names_against_injection():
    malicious = pd.DataFrame([
        {"flight_date": "2026-08-15", "origin_country": "<script>alert(1)</script>", "total_flights": 1,
         "avg_velocity": 1.0, "peak_velocity": 1.0, "avg_altitude": 1.0,
         "total_on_ground": 0, "rank_by_volume": 1},
    ])
    empty = pd.DataFrame(columns=["origin_country", "flights_last_24h", "avg_velocity_last_24h", "last_loaded_at"])
    out = render_dashboard_html(malicious, empty, datetime.now(timezone.utc))
    assert "<script>alert(1)</script>" not in out
