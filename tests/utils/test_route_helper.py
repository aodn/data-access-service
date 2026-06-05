import json
from pathlib import Path

import pandas as pd
import xarray

from data_access_service.models.value_count import ValueCount
from data_access_service.utils.routes_helper import (
    round_coordinate_list,
    generate_feature_collection,
    generate_rect_features,
)


def test_round_coordinate_list():

    coords = [1.234567, 2.345678, 3.456789]
    rounded_coords = round_coordinate_list(coords)
    expected_coords = [
        ValueCount(value=1.2, count=1),
        ValueCount(value=2.3, count=1),
        ValueCount(value=3.5, count=1),
    ]
    assert rounded_coords == expected_coords

    coords2 = [1.234567, 2.345678, 3.456789, 1.23]
    rounded_coords2 = round_coordinate_list(coords2)
    expected_coords2 = [
        ValueCount(value=1.2, count=2),
        ValueCount(value=2.3, count=1),
        ValueCount(value=3.5, count=1),
    ]
    assert rounded_coords2 == expected_coords2


def test_generate_feature_collection():
    zarr_path = (
        Path(__file__).parent.parent
        / "canned/s3_sample3/satellite_ghrsst_l4_gamssa_1day_multi_sensor_world.zarr"
    )
    if not zarr_path.exists():
        raise FileNotFoundError(f"Test data file not found: {zarr_path}")
    dataset = xarray.open_zarr(zarr_path)
    start_date = pd.Timestamp("2008-08-01 12:00:00.000000000")
    end_date = pd.Timestamp("2008-08-31 23:59:59.999999999")
    subset = dataset.sel(
        lat=slice(-0.3, 0.3), lon=slice(-0.3, 0.3), time=slice(start_date, end_date)
    )
    feature_collection = generate_feature_collection(subset, "lat", "lon", "time")
    expected_result_path = (
        Path(__file__).parent.parent
        / "canned/expected_json/generate_feature_collection_expected.json"
    )
    with open(expected_result_path) as file:
        expected_result = file.read()

    actual_result = json.dumps(feature_collection)
    assert json.loads(actual_result) == json.loads(expected_result)


def test_generate_rect_feature_collection():
    zarr_path = (
        Path(__file__).parent.parent
        / "canned/s3_sample2/satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr"
    )
    if not zarr_path.exists():
        raise FileNotFoundError(f"Test data file not found: {zarr_path}")
    dataset = xarray.open_zarr(zarr_path)
    start_date = pd.Timestamp("2011-11-01 00:00:00.000000000")
    end_date = pd.Timestamp("2011-11-30 23:59:59.999999999")
    subset = dataset.sel(time=slice(start_date, end_date))
    feature_collection = generate_rect_features(subset, "lat", "lon", "time")
    expected_result_path = (
        Path(__file__).parent.parent
        / "canned/expected_json/generate_rect_feature_collection_expected.json"
    )
    with open(expected_result_path) as file:
        expected_result = file.read()

    actual_result = json.dumps(feature_collection)
    assert json.loads(actual_result) == json.loads(expected_result)


def test_generate_rect_features_scalar_coords():
    # Create a mock xarray.Dataset with scalar (0-dimensional) lat and lon coordinates,
    # and a 1-dimensional time coordinate of length 1.
    dataset = xarray.Dataset(
        coords={
            "lat": ([], -32.0),
            "lon": ([], 115.0),
            "time": (["time"], [pd.Timestamp("2026-06-05")]),
        }
    )

    # Verify that the coordinate values are indeed 0-dimensional (scalar) arrays
    assert dataset.coords["lat"].values.ndim == 0
    assert dataset.coords["lon"].values.ndim == 0

    # Call generate_rect_features; this should not throw an exception (such as TypeError: len() of unsized object)
    features = generate_rect_features(dataset, "lat", "lon", "time")

    # Verify the results are correct
    assert features is not None
    assert len(features) == 1

    feature = features[0]
    assert feature["geometry"]["type"] == "Polygon"
    assert feature["properties"]["date"] == "2026-06"
    assert feature["properties"]["count"] == 1  # 1 * 1 * 1

    # Verify the coordinates form a correct bounding rectangle
    expected_polygon = [
        [115.0, -32.0],
        [115.0, -32.0],
        [115.0, -32.0],
        [115.0, -32.0],
        [115.0, -32.0],
    ]
    assert feature["geometry"]["coordinates"] == [expected_polygon]
