"""
Run with: pytest tests/test_snowflake_implement.py -v

Covers a bug found against the live OpenSky feed: gold's avg_altitude is NaN
for any (window, country) group where every aircraft had a null baro_altitude
(e.g. grounded aircraft that only report position). float('nan') passed as a
Snowflake bind parameter renders as the bare token NAN in the generated SQL,
which Snowflake parses as an invalid identifier rather than a float literal —
_nullable_float converts NaN to None (SQL NULL) instead.
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.snowflake_implement import _nullable_float


def test_nan_becomes_none():
    assert _nullable_float(float("nan")) is None


def test_real_value_passes_through_as_float():
    assert _nullable_float(245.3) == 245.3


def test_int_value_passes_through_as_float():
    assert _nullable_float(0) == 0.0
