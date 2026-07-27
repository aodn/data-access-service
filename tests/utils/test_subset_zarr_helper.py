"""Unit tests for subset_zarr (utils.subset_zarr_helper) - the single shared
owner of "apply one time range + N bboxes to a zarr dataset".

The multi-polygon download only contained polygon 1's data. subset_zarr now
applies all bboxes in one pass (union-grid .isel() + one OR'd .where() mask).
"""

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from unittest.mock import MagicMock

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.subset_zarr_helper import subset_zarr

UUID = "test-uuid"
KEY = "test-key"

START = pd.Timestamp("2020-01-01", tz="UTC")
END = pd.Timestamp("2020-01-02 23:59:59", tz="UTC")


def _api():
    """subset_zarr only needs api.map_column_names; the fixtures name their
    dims TIME/LATITUDE/LONGITUDE, so an identity map resolves them."""
    api = MagicMock()
    api.map_column_names = MagicMock(
        side_effect=lambda uuid, key, columns: list(columns)
    )
    return api


def _regular_grid() -> xr.Dataset:
    """4 time steps x 10 lat x 10 lon, values 0..399 so cells are identifiable."""
    return xr.Dataset(
        {
            "sst": (
                ("TIME", "LATITUDE", "LONGITUDE"),
                np.arange(400.0, dtype="float64").reshape(4, 10, 10),
            )
        },
        coords={
            "TIME": pd.date_range("2020-01-01", periods=4),
            "LATITUDE": np.arange(10.0),
            "LONGITUDE": np.arange(10.0),
        },
    )


def _curvilinear_grid() -> xr.Dataset:
    """Curvilinear: dims are (TIME, I, J); LATITUDE/LONGITUDE are 2D variables."""
    ii, jj = 4, 6
    lat2d, lon2d = np.meshgrid(
        np.linspace(0, 9, ii), np.linspace(0, 9, jj), indexing="ij"
    )
    return xr.Dataset(
        {
            "temp": (("TIME", "I", "J"), np.ones((4, ii, jj), dtype="float32")),
            "LATITUDE": (("I", "J"), lat2d),
            "LONGITUDE": (("I", "J"), lon2d),
        },
        coords={"TIME": pd.date_range("2020-01-01", periods=4)},
    )


# bbox args are (min_lon, min_lat, max_lon, max_lat)
BOX_LOW = BoundingBox(0, 0, 2, 2)  # lat/lon [0, 2]
BOX_HIGH = BoundingBox(7, 7, 9, 9)  # lat/lon [7, 9]


def _run(dataset, bboxes, **kwargs):
    return subset_zarr(dataset, _api(), UUID, KEY, START, END, bboxes, **kwargs)


def test_single_bbox_unchanged_from_plain_sel():
    # Regression guard: one bbox on a regular grid must behave exactly like the
    # old .sel() slicing - same block, no mask, no NaN, no dtype promotion.
    ds = _regular_grid()

    subset = _run(ds, [BOX_LOW])

    oracle = ds.sel(
        TIME=slice(START.tz_localize(None), END.tz_localize(None)),
        LATITUDE=slice(0, 2),
        LONGITUDE=slice(0, 2),
    )
    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 3, "LONGITUDE": 3}
    assert subset.sst.dtype == ds.sst.dtype
    assert not np.isnan(subset.sst.values).any()
    np.testing.assert_array_equal(subset.sst.values, oracle.sst.values)


def test_disjoint_bboxes_keep_both_boxes_data():
    # THE #8499 regression: the second box's data must be real, not NaN (the
    # old merge dropped it), and the grid must be the union of the two boxes'
    # positions (6x6), not the full envelope (10x10).
    ds = _regular_grid()

    subset = _run(ds, [BOX_LOW, BOX_HIGH])

    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 6, "LONGITUDE": 6}
    t0 = subset.sst.isel(TIME=0)
    block_low = t0.isel(LATITUDE=slice(0, 3), LONGITUDE=slice(0, 3))
    block_high = t0.isel(LATITUDE=slice(3, 6), LONGITUDE=slice(3, 6))
    cross = t0.isel(LATITUDE=slice(0, 3), LONGITUDE=slice(3, 6))

    assert not np.isnan(block_low.values).any(), "box 1 data lost"
    assert not np.isnan(block_high.values).any(), "box 2 data lost (#8499)"
    np.testing.assert_array_equal(
        block_low.values,
        ds.sst.isel(TIME=0, LATITUDE=slice(0, 3), LONGITUDE=slice(0, 3)).values,
    )
    np.testing.assert_array_equal(
        block_high.values,
        ds.sst.isel(TIME=0, LATITUDE=slice(7, 10), LONGITUDE=slice(7, 10)).values,
    )
    # the "cross" cells (box 1's lats x box 2's lons) belong to neither box
    assert np.isnan(cross.values).all(), "cells outside every box must be NaN"


def test_overlapping_bboxes_counted_once():
    # lat/lon [0,2] + [1,4] -> union [0,4]: 5 positions, not 3 + 4.
    ds = _regular_grid()

    subset = _run(ds, [BOX_LOW, BoundingBox(1, 1, 4, 4)])

    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 5, "LONGITUDE": 5}


def test_descending_lat_axis():
    # A descending lat axis (real stores have these) must select the same rows;
    # the old .sel(slice) needed a manual swap, value comparison does not.
    ds = _regular_grid().isel(LATITUDE=slice(None, None, -1))

    subset = _run(ds, [BOX_LOW])

    assert sorted(subset.LATITUDE.values) == [0.0, 1.0, 2.0]


def test_bbox_outside_grid_is_empty():
    ds = _regular_grid()

    subset = _run(ds, [BoundingBox(50, 50, 60, 60)])

    assert subset.sizes["LATITUDE"] == 0
    assert subset.sizes["LONGITUDE"] == 0


def test_dask_backed_stays_lazy():
    # Nothing may compute: the download chunks by time and writes lazily.
    ds = _regular_grid().chunk({"TIME": 1})

    subset = _run(ds, [BOX_LOW, BOX_HIGH])

    assert isinstance(subset.sst.data, da.Array)


def test_apply_mask_false_keeps_shape_and_dtype():
    # The size estimate skips the .where() (eager on a non-dask store -> OOM)
    # and relies on drop=False never changing the shape: nbytes must match the
    # masked download slice exactly.
    ds = _regular_grid()

    masked = _run(ds, [BOX_LOW, BOX_HIGH], apply_mask=True)
    unmasked = _run(ds, [BOX_LOW, BOX_HIGH], apply_mask=False)

    assert dict(unmasked.sizes) == dict(masked.sizes)
    assert int(unmasked.nbytes) == int(masked.nbytes)
    # unmasked skips .where() entirely, so no NaN blanking and no promotion
    assert not np.isnan(unmasked.sst.values).any()


def test_curvilinear_multi_bbox_extends_mask_not_shape():
    # 2D LATITUDE/LONGITUDE vars can't be indexed by value: the grid shape must
    # stay I x J, and a second bbox must ADD surviving cells via the OR'd mask.
    ds = _curvilinear_grid()

    one = _run(ds, [BOX_LOW])
    two = _run(ds, [BOX_LOW, BOX_HIGH])

    assert dict(one.sizes) == {"TIME": 2, "I": 4, "J": 6}
    assert dict(two.sizes) == {"TIME": 2, "I": 4, "J": 6}
    kept_one = int(np.isfinite(one.temp.isel(TIME=0)).sum())
    kept_two = int(np.isfinite(two.temp.isel(TIME=0)).sum())
    assert kept_one > 0
    assert kept_two > kept_one, "second box must add cells on a curvilinear grid"


def test_empty_bboxes_raises():
    # Empty means the caller skipped ResolvedSubsetRequest.effective_bboxes
    # (which defaults to the whole globe) - fail loudly, don't return everything.
    with pytest.raises(ValueError, match="at least one bbox"):
        _run(_regular_grid(), [])


def test_unknown_condition_name_raises():
    # A dataset without the resolved lat dim -> neither dim nor var -> ValueError.
    ds = xr.Dataset(
        {"v": ("TIME", np.zeros(4))},
        coords={"TIME": pd.date_range("2020-01-01", periods=4)},
    )
    with pytest.raises(ValueError, match="neither dim"):
        _run(ds, [BOX_LOW])
