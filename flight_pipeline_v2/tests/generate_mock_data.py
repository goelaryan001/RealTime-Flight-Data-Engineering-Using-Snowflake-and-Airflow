"""
Generates a realistic mock OpenSky /states/all API response for local testing.
Real OpenSky state vectors are 17-element arrays in this exact order:
[icao24, callsign, origin_country, time_position, last_contact, longitude,
 latitude, baro_altitude, on_ground, velocity, true_track, vertical_rate,
 sensors, geo_altitude, squawk, spi, position_source]

This mock includes deliberately messy/realistic cases:
- Some null velocity/altitude (aircraft on ground or lost signal)
- Some null callsign
- A duplicate icao24 (simulating overlapping poll windows)
- A stale time_position (simulating a delayed report)
- Negative/impossible velocity (bad sensor data) to test validation
"""
import json
import time
from pathlib import Path

now = int(time.time())

states = [
    # icao24, callsign, origin_country, time_position, last_contact, lon, lat, baro_alt, on_ground, velocity, true_track, vert_rate, sensors, geo_alt, squawk, spi, pos_source
    ["a1b2c3", "UAL123  ", "United States", now, now, -122.41, 37.77, 10668.0, False, 245.3, 270.5, 0.0, None, 10972.8, "1200", False, 0],
    ["d4e5f6", "AFR456  ", "France", now, now, 2.35, 48.85, 11582.4, False, 251.7, 90.2, -1.5, None, 11887.2, "2000", False, 0],
    ["g7h8i9", "DLH789  ", "Germany", now, now, 13.4, 52.52, 9753.6, False, 238.9, 180.0, 0.0, None, 10058.4, "1000", False, 0],
    ["j1k2l3", None, "United States", now, now, -87.65, 41.85, 0.0, True, 0.0, 0.0, 0.0, None, 0.0, "1200", False, 0],  # on ground, null callsign
    ["m4n5o6", "SIA321  ", "Singapore", now, now, 103.82, 1.35, 12192.0, False, 267.1, 45.0, 0.0, None, 12496.8, "3000", False, 0],
    ["p7q8r9", "QFA654  ", "Australia", now, now, 151.21, -33.87, None, False, None, None, None, None, None, "5000", False, 0],  # lost signal — nulls
    ["s1t2u3", "ACA987  ", "Canada", now, now, -79.38, 43.65, 10363.2, False, 242.0, 315.0, 2.1, None, 10668.0, "4000", False, 0],
    ["a1b2c3", "UAL123  ", "United States", now, now, -122.41, 37.77, 10668.0, False, 245.3, 270.5, 0.0, None, 10972.8, "1200", False, 0],  # duplicate row
    ["v4w5x6", "BAW111  ", "United Kingdom", now - 1800, now, -0.13, 51.51, 11277.6, False, 245.0, 200.0, 0.0, None, 11582.4, "6000", False, 0],  # stale: time_position 30min behind last_contact, otherwise plausible
    ["y7z8a1", "JAL222  ", "Japan", now, now, 139.69, 35.68, 10972.8, False, 255.6, 60.0, 0.0, None, 11277.6, "7000", False, 0],
    ["b2c3d4", "United States", "United States", now, now, -95.36, 29.76, 9144.0, False, 229.4, 135.0, -0.5, None, 9448.8, "1200", False, 0],
    ["e5f6g7", "ANA333  ", "Japan", now, now, 135.5, 34.69, 10668.0, False, 248.2, 225.0, 0.0, None, 10972.8, "7100", False, 0],
    ["c9d8e7", "SWA555  ", "United States", now, now, -104.99, 39.74, 10058.4, False, 999.9, 90.0, 0.0, None, 10363.2, "8000", False, 0],  # impossible velocity, otherwise clean
]

payload = {"time": now, "states": states}

out_path = Path("/home/claude/flight_pipeline_v2/tests/mock_opensky_response.json")
out_path.write_text(json.dumps(payload, indent=2))
print(f"Wrote {len(states)} mock state vectors to {out_path}")
print(f"Includes: 1 duplicate row, 1 fully-null row, 1 impossible-velocity row, 1 stale timestamp row")
