"""Unit tests for the GeoJSON transforms in data_access_service.sites.geojson.

Pure data transforms — every function takes a pandas DataFrame (a repository
read's output) and returns a GeoJSON dict. No DuckDB or S3 involved here.
"""

import pandas as pd

from data_access_service.sites.geojson import (
    _iso,
    _native,
    _point,
    _series,
    site_details_feature_collection,
    site_feature_collection,
)


# --- small helpers -----------------------------------------------------------


def test_native_unwraps_numpy_scalar():
    value = pd.Series([5], dtype="int64").iloc[0]
    out = _native(value)
    assert out == 5
    assert isinstance(out, int)  # plain python, not numpy


def test_native_passes_through_plain_scalar():
    assert _native("abc") == "abc"


def test_iso_uses_isoformat_when_available():
    assert _iso(pd.Timestamp("2024-01-01T00:00:00")) == "2024-01-01T00:00:00"


def test_iso_falls_back_to_str():
    assert _iso("2024-01-01") == "2024-01-01"


def test_point_is_lon_lat_order():
    point = _point(150.0, -30.0)
    assert point == {"type": "Point", "coordinates": [150.0, -30.0]}


# --- _series -----------------------------------------------------------------


def test_series_builds_ms_epoch_pairs():
    frame = pd.DataFrame(
        {
            "TIME": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "TEMP": [20.0, 21.0],
        }
    )
    series = _series(frame, ["TEMP"], "TIME")
    expected_first_ms = int(pd.Timestamp("2024-01-01").value // 1_000_000)
    assert series["TEMP"][0] == (expected_first_ms, 20.0)
    assert len(series["TEMP"]) == 2


def test_series_drops_nulls_per_column():
    frame = pd.DataFrame(
        {
            "TIME": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "TEMP": [20.0, None, 22.0],
            "PSAL": [None, 35.0, 34.0],
        }
    )
    series = _series(frame, ["TEMP", "PSAL"], "TIME")
    # NULLs dropped independently per column, so the two series differ in length
    assert [v for _, v in series["TEMP"]] == [20.0, 22.0]
    assert [v for _, v in series["PSAL"]] == [35.0, 34.0]


# --- site_feature_collection -------------------------------------------------


def test_site_feature_collection_one_feature_per_site():
    rows = pd.DataFrame(
        {
            "site_code": ["A", "B"],
            "latitude": [-30.0, -32.0],
            "longitude": [150.0, 151.0],
            "time": pd.to_datetime(["2024-01-02", "2024-01-03"]),
        }
    )
    fc = site_feature_collection(rows, site_column="site_code")
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 2

    first = fc["features"][0]
    assert first["type"] == "Feature"
    assert first["properties"] == {"date": "2024-01-02T00:00:00", "site": "A"}
    # GeoJSON is [lon, lat]
    assert first["geometry"]["coordinates"] == [150.0, -30.0]
    # _id is injected by the frontend, never part of the payload
    assert "_id" not in first["properties"]


def test_site_feature_collection_empty():
    rows = pd.DataFrame({"site_code": [], "latitude": [], "longitude": [], "time": []})
    fc = site_feature_collection(rows, site_column="site_code")
    assert fc == {"type": "FeatureCollection", "features": []}


# --- site_details_feature_collection -----------------------------------------


def _details_rows():
    return pd.DataFrame(
        {
            "TIME": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01"]),
            "LATITUDE": [-30.0, -30.0, -30.0],
            "LONGITUDE": [150.0, 150.0, 150.0],
            "NOMINAL_DEPTH": [10, 10, 20],
            "TEMP": [20.0, 21.0, 18.0],
        }
    )


def test_details_grouped_nests_series_per_group():
    feature = site_details_feature_collection(
        _details_rows(),
        time_column="TIME",
        longitude_column="LONGITUDE",
        latitude_column="LATITUDE",
        value_columns=["TEMP"],
        group_column="NOMINAL_DEPTH",
    )
    assert feature["type"] == "Feature"
    assert feature["geometry"]["coordinates"] == [150.0, -30.0]
    # one series object per distinct depth, keyed "<group>_<value>"
    assert set(feature["properties"]) == {"NOMINAL_DEPTH_10", "NOMINAL_DEPTH_20"}
    assert len(feature["properties"]["NOMINAL_DEPTH_10"]["TEMP"]) == 2
    assert len(feature["properties"]["NOMINAL_DEPTH_20"]["TEMP"]) == 1


def test_details_ungrouped_series_directly_in_properties():
    rows = pd.DataFrame(
        {
            "TIME": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "LATITUDE": [-32.0, -32.0],
            "LONGITUDE": [151.0, 151.0],
            "WHTH": [1.5, 1.6],
        }
    )
    feature = site_details_feature_collection(
        rows,
        time_column="TIME",
        longitude_column="LONGITUDE",
        latitude_column="LATITUDE",
        value_columns=["WHTH"],
        group_column=None,
    )
    # series sit directly under properties (no depth nesting)
    assert "WHTH" in feature["properties"]
    assert len(feature["properties"]["WHTH"]) == 2


def test_details_excludes_group_column_from_values():
    feature = site_details_feature_collection(
        _details_rows(),
        time_column="TIME",
        longitude_column="LONGITUDE",
        latitude_column="LATITUDE",
        # group column passed in value_columns must be dropped, not emitted as a series
        value_columns=["TEMP", "NOMINAL_DEPTH"],
        group_column="NOMINAL_DEPTH",
    )
    one_group = feature["properties"]["NOMINAL_DEPTH_10"]
    assert "NOMINAL_DEPTH" not in one_group
    assert set(one_group) == {"TEMP"}


def test_details_empty_frame_has_null_geometry():
    rows = pd.DataFrame(
        {
            "TIME": pd.to_datetime([]),
            "LATITUDE": [],
            "LONGITUDE": [],
            "NOMINAL_DEPTH": [],
            "TEMP": [],
        }
    )
    feature = site_details_feature_collection(
        rows,
        time_column="TIME",
        longitude_column="LONGITUDE",
        latitude_column="LATITUDE",
        value_columns=["TEMP"],
        group_column="NOMINAL_DEPTH",
    )
    assert feature == {"type": "Feature", "properties": {}, "geometry": None}
