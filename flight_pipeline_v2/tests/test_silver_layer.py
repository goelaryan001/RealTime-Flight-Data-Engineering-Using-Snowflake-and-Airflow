"""
Run with: pytest tests/test_silver_layer.py -v
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.silver_layer import clean_flight_data


@pytest.fixture
def mock_raw_data():
    mock_file = Path(__file__).parent / "mock_opensky_response.json"
    with open(mock_file) as f:
        return json.load(f)


def test_removes_exact_duplicates(mock_raw_data):
    df = clean_flight_data(mock_raw_data)
    assert list(df["icao24"]).count("a1b2c3") == 1


def test_drops_fully_null_aircraft(mock_raw_data):
    df = clean_flight_data(mock_raw_data)
    assert "p7q8r9" not in set(df["icao24"])


def test_drops_implausible_velocity(mock_raw_data):
    df = clean_flight_data(mock_raw_data)
    assert "c9d8e7" not in set(df["icao24"])
    assert (df["velocity"] <= 350.0).all()


def test_drops_stale_reports(mock_raw_data):
    df = clean_flight_data(mock_raw_data)
    assert "v4w5x6" not in set(df["icao24"])


def test_drops_null_origin_country(mock_raw_data):
    df = clean_flight_data(mock_raw_data)
    assert "n2o3p4" not in set(df["icao24"])
    assert df["origin_country"].isna().sum() == 0


def test_retains_geospatial_and_timestamp_columns(mock_raw_data):
    df = clean_flight_data(mock_raw_data)
    for col in ["longitude", "latitude", "baro_altitude", "time_position"]:
        assert col in df.columns


def test_raises_on_empty_states(mock_raw_data):
    empty_payload = {"time": mock_raw_data["time"], "states": []}
    with pytest.raises(ValueError):
        clean_flight_data(empty_payload)


def test_raises_on_missing_states_key():
    with pytest.raises(ValueError):
        clean_flight_data({"time": 123})
