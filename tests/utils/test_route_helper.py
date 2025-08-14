import json
from pathlib import Path

import pandas as pd
import xarray

from data_access_service.models.value_count import ValueCount
from data_access_service.utils.routes_helper import (
    round_coordinate_list,
    generate_feature_collection,
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

    actual_result = json.dumps(feature_collection.to_dict())
    assert json.loads(actual_result) == json.loads(expected_result)
