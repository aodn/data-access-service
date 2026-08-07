"""The vectorised date index must match the per-timestamp implementation exactly.

Building sixty of these at startup is why it was vectorised; the date strings it
produces are matched literally against user-supplied dates on every request, so
"faster" is only acceptable if it is also byte-identical. DST transitions are
where a bulk tz conversion is most likely to diverge from a per-value one.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.tiler.services.store.registry import _build_date_index
from data_access_service.tiler.utils.dates import ts_to_local_date


def _reference_index(ds: xr.Dataset) -> dict[str, list]:
    """The original per-timestamp implementation, kept as the oracle."""
    if "time" not in ds.dims:
        return {}
    index: dict[str, list] = {}
    for ts in ds.coords["time"].values:
        index.setdefault(ts_to_local_date(ts), []).append(ts)
    return index


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


def _assert_matches_reference(ds: xr.Dataset) -> dict[str, list]:
    actual = _build_date_index(ds)
    expected = _reference_index(ds)

    assert actual.keys() == expected.keys()
    for date in expected:
        assert list(actual[date]) == list(expected[date])
    return actual


# --- equivalence ------------------------------------------------------------


def test_matches_reference_on_hourly_data():
    times = pd.date_range("2024-06-01", periods=72, freq="h").values
    _assert_matches_reference(_ds(times))


def test_matches_reference_on_daily_data():
    times = pd.date_range("2024-01-01", periods=400, freq="D").values
    _assert_matches_reference(_ds(times))


@pytest.mark.parametrize(
    "start,label",
    [
        # Australia/Sydney: DST ends the first Sunday of April (clocks back),
        # starts the first Sunday of October (clocks forward). Both are 2am
        # local, i.e. mid-UTC-day, so a whole local date straddles the shift.
        ("2024-04-06", "dst_end_autumn"),
        ("2024-10-05", "dst_start_spring"),
        ("2025-04-05", "dst_end_2025"),
        ("2025-10-04", "dst_start_2025"),
    ],
)
def test_matches_reference_across_dst_transitions(start, label):
    times = pd.date_range(start, periods=96, freq="h").values
    index = _assert_matches_reference(_ds(times))
    # Sanity: the window really does span the transition, so this is not
    # vacuously passing on a uniform-offset range.
    assert len(index) >= 4


def test_matches_reference_when_timestamps_share_a_local_date():
    """Several UTC timestamps collapsing onto one local date must stay grouped
    in coord order — load_slice takes the first."""
    times = pd.date_range("2024-06-01 00:00", periods=24, freq="h").values
    index = _assert_matches_reference(_ds(times))
    assert any(len(v) > 1 for v in index.values())


def test_matches_reference_on_utc_midnight_boundaries():
    """UTC midnight is mid-morning local, so the local date has already rolled
    over — the classic off-by-one this conversion has to get right."""
    times = pd.to_datetime(
        [
            "2024-06-01 13:59:59",
            "2024-06-01 14:00:00",
            "2024-06-01 23:59:59",
            "2024-06-02 00:00:00",
        ]
    ).values
    _assert_matches_reference(_ds(times))


# --- edge cases -------------------------------------------------------------


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
    assert _build_date_index(ds) == {}


def test_empty_time_coord_yields_empty_index():
    times = np.array([], dtype="datetime64[ns]")
    assert _build_date_index(_ds(times)) == {}


def test_single_timestamp():
    _assert_matches_reference(_ds(pd.to_datetime(["2024-06-01 03:00"]).values))


def test_stored_values_are_the_raw_coord_elements():
    """load_slice selects with these, so they must be the coord's own values,
    not the converted local dates."""
    times = pd.date_range("2024-06-01", periods=3, freq="D").values
    index = _build_date_index(_ds(times))
    stored = [ts for values in index.values() for ts in values]
    assert all(isinstance(ts, np.datetime64) for ts in stored)
    assert sorted(stored) == sorted(times)
