"""The per-store time index must map each raw store timestamp to itself,
keyed by the exact instant a parsed client request would produce — no
calendar-day bucketing, no timezone conversion.
"""

import numpy as np
import pandas as pd
import xarray as xr

from data_access_service.tiler.services.store.registry import _build_time_index


def _ds(times) -> xr.Dataset:
    times = np.asarray(times)
    return xr.Dataset(
        {
            "var": xr.DataArray(
                np.zeros((len(times), 2, 2)),
                dims=("time", "lat", "lon"),
                coords={"time": times, "lat": [0.0, 1.0], "lon": [0.0, 1.0]},
            )
        }
    )


def test_keys_are_parsed_timestamps_of_the_raw_coord_values():
    times = pd.date_range("2024-06-01", periods=3, freq="D").values
    index = _build_time_index(_ds(times))
    assert set(index.keys()) == {pd.Timestamp(ts) for ts in times}


def test_values_are_the_raw_coord_elements():
    """load_slice selects with these, so they must be the coord's own values."""
    times = pd.date_range("2024-06-01", periods=3, freq="D").values
    index = _build_time_index(_ds(times))
    assert all(isinstance(ts, np.datetime64) for ts in index.values())
    assert sorted(index.values()) == sorted(times)


def test_each_timestamp_is_its_own_key_no_bucketing():
    """Sub-daily timestamps must each be independently addressable — no
    collapsing onto a shared day key."""
    times = pd.date_range("2024-06-01 00:00", periods=24, freq="h").values
    index = _build_time_index(_ds(times))
    assert len(index) == 24


def test_lookup_by_parsed_client_string_resolves_to_raw_value():
    times = pd.to_datetime(["2022-05-31 15:20:00"]).values
    index = _build_time_index(_ds(times))
    from data_access_service.tiler.utils.dates import parse_query_date

    assert index[parse_query_date("2022-05-31T15:20:00Z")] == times[0]


def test_store_without_time_dimension_yields_empty_index():
    ds = xr.Dataset(
        {
            "var": xr.DataArray(
                np.zeros((2, 2)),
                dims=("lat", "lon"),
                coords={"lat": [0.0, 1.0], "lon": [0.0, 1.0]},
            )
        }
    )
    assert _build_time_index(ds) == {}


def test_empty_time_coord_yields_empty_index():
    times = np.array([], dtype="datetime64[ns]")
    assert _build_time_index(_ds(times)) == {}


def test_single_timestamp():
    times = pd.to_datetime(["2024-06-01 03:00"]).values
    index = _build_time_index(_ds(times))
    assert index == {pd.Timestamp(times[0]): times[0]}
