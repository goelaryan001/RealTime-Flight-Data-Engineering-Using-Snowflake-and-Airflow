"""
Run with: pytest tests/test_data_quality.py -v
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.silver_layer import clean_flight_data
from scripts.data_quality import run_quality_checks


@pytest.fixture
def clean_silver_df():
    mock_file = Path(__file__).parent / "mock_opensky_response.json"
    with open(mock_file) as f:
        raw = json.load(f)
    return clean_flight_data(raw)


def test_passes_on_clean_data(clean_silver_df):
    run_quality_checks(clean_silver_df)  # should not raise


def test_catches_duplicate_icao24(clean_silver_df):
    dupe_df = pd.concat([clean_silver_df, clean_silver_df.iloc[[0]]], ignore_index=True)
    with pytest.raises(AssertionError, match="Duplicate icao24"):
        run_quality_checks(dupe_df)


def test_catches_negative_velocity(clean_silver_df):
    bad_df = clean_silver_df.copy()
    bad_df.loc[0, "velocity"] = -50.0
    with pytest.raises(AssertionError, match="negative velocity"):
        run_quality_checks(bad_df)


def test_catches_empty_dataframe():
    with pytest.raises(AssertionError, match="expected at least"):
        run_quality_checks(pd.DataFrame(columns=["icao24", "origin_country", "velocity", "on_ground", "time_position"]))


def test_catches_missing_required_column(clean_silver_df):
    bad_df = clean_silver_df.drop(columns=["origin_country"])
    with pytest.raises(AssertionError, match="missing required columns"):
        run_quality_checks(bad_df)
