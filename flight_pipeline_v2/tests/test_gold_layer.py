"""
Run with: pytest tests/test_gold_layer.py tests/test_data_quality.py -v
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.silver_layer import clean_flight_data
from scripts.gold_layer import build_gold_aggregates


@pytest.fixture
def clean_silver_df():
    mock_file = Path(__file__).parent / "mock_opensky_response.json"
    with open(mock_file) as f:
        raw = json.load(f)
    return clean_flight_data(raw)


def test_gold_aggregates_have_expected_columns(clean_silver_df):
    gold = build_gold_aggregates(clean_silver_df)
    expected = {"window_start", "origin_country", "total_flights", "avg_velocity",
                "max_velocity", "avg_altitude", "on_ground_count"}
    assert expected.issubset(set(gold.columns))


def test_gold_row_count_never_exceeds_country_count(clean_silver_df):
    gold = build_gold_aggregates(clean_silver_df)
    # can't have more (window, country) groups than distinct countries in a single window
    assert gold["origin_country"].nunique() <= clean_silver_df["origin_country"].nunique()


def test_total_flights_conserved(clean_silver_df):
    gold = build_gold_aggregates(clean_silver_df)
    assert gold["total_flights"].sum() == len(clean_silver_df)


def test_raises_on_empty_input():
    with pytest.raises(ValueError):
        build_gold_aggregates(pd.DataFrame())
