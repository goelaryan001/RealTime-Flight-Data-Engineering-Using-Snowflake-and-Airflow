"""
Run with: pytest tests/test_dashboard.py -v
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.dashboard import render_dashboard_html, build_country_index, TOP_N


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


def test_search_box_present(country_summary, rolling_24h):
    out = render_dashboard_html(country_summary, rolling_24h, datetime.now(timezone.utc))
    assert 'id="countrySearch"' in out
    assert "const COUNTRY_DATA" in out


def test_country_index_merges_both_views(country_summary, rolling_24h):
    index = build_country_index(country_summary, rolling_24h)
    # United States is in both views - fields from each should both be present
    assert index["United States"]["total_flights_today"] == 5310
    assert index["United States"]["flights_last_24h"] == 5310
    # Canada is only in country_summary (not in the rolling_24h fixture) -
    # it should still get an entry, just without the rolling-window fields
    assert index["Canada"]["total_flights_today"] == 458
    assert "flights_last_24h" not in index["Canada"]


def test_search_covers_countries_beyond_the_chart_top_n():
    # 12 countries - more than TOP_N (10) - the charts only show the top N,
    # but the search box is supposed to cover every country, not just those
    many = pd.DataFrame([
        {"flight_date": "2026-08-15", "origin_country": f"Country{i}", "total_flights": 100 - i,
         "avg_velocity": 50.0, "peak_velocity": 60.0, "avg_altitude": 1000.0,
         "total_on_ground": 1, "rank_by_volume": i + 1}
        for i in range(12)
    ])
    empty_rolling = pd.DataFrame(columns=["origin_country", "flights_last_24h", "avg_velocity_last_24h", "last_loaded_at"])
    assert len(many) > TOP_N
    out = render_dashboard_html(many, empty_rolling, datetime.now(timezone.utc))
    # the 12th-ranked country wouldn't make a top-10 chart, but must still be searchable
    assert "Country11" in out


def test_search_json_escapes_against_script_breakout():
    malicious = pd.DataFrame([
        {"flight_date": "2026-08-15", "origin_country": "</script><script>alert(1)</script>", "total_flights": 1,
         "avg_velocity": 1.0, "peak_velocity": 1.0, "avg_altitude": 1.0,
         "total_on_ground": 0, "rank_by_volume": 1},
    ])
    empty = pd.DataFrame(columns=["origin_country", "flights_last_24h", "avg_velocity_last_24h", "last_loaded_at"])
    out = render_dashboard_html(malicious, empty, datetime.now(timezone.utc))
    assert "</script><script>" not in out
