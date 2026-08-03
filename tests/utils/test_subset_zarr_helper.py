"""Unit tests for subset_zarr (utils.subset_zarr_helper) - the single shared
owner of "apply one time range + the requested area to a zarr dataset".

The multi-polygon download only contained polygon 1's data. subset_zarr now
crops every requested position in one pass (union-grid .isel()) and blanks the
cells the crop had to include with one point-in-polygon .where() mask.
"""

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import box as shapely_box

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.multi_polygon_helper import bbox_of
from data_access_service.utils.subset_zarr_helper import area_to_keep, subset_zarr

KEY = "test-key"

START = pd.Timestamp("2020-01-01", tz="UTC")
END = pd.Timestamp("2020-01-02 23:59:59", tz="UTC")


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


def _trajectory() -> xr.Dataset:
    """A ship track: LATITUDE and LONGITUDE are both 1D variables along TIME, so
    each time step is ONE point - not a lat x lon plane (real store:
    vessel_satellite_radiance_delayed_qc.zarr)."""
    return xr.Dataset(
        {
            "radiance": ("TIME", np.arange(4.0)),
            "LATITUDE": ("TIME", np.array([0.0, 1.0, 8.0, 9.0])),
            "LONGITUDE": ("TIME", np.array([0.0, 1.0, 8.0, 9.0])),
        },
        coords={"TIME": pd.date_range("2020-01-01", periods=4)},
    )


# bbox args are (min_lon, min_lat, max_lon, max_lat)
BOX_LOW = BoundingBox(0, 0, 2, 2)  # lat/lon [0, 2]
BOX_HIGH = BoundingBox(7, 7, 9, 9)  # lat/lon [7, 9]


def _run(dataset, bboxes, **kwargs):
    return subset_zarr(
        dataset, KEY, "LATITUDE", "LONGITUDE", "TIME", START, END, bboxes, **kwargs
    )


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


def test_no_bbox_is_not_a_mask_on_a_curvilinear_grid():
    # "No area given" stays an empty bbox list all the way down. On 1D lat/lon
    # exact_crop already returns None, but 2D lat/lon cannot be cropped at all,
    # so without the empty guard every cell would be point-in-polygon tested.
    ds = _curvilinear_grid()

    assert area_to_keep(ds, "LATITUDE", "LONGITUDE", [], None) is None


def test_no_bbox_does_not_wipe_a_0_360_store():
    # A whole-globe -180..180 rectangle does not contain lon=200, so if "no area"
    # became such a box, masking a store whose longitudes run 0..360 with it
    # would NaN out every cell.
    ds = _curvilinear_grid()
    ds["LONGITUDE"] = ds.LONGITUDE + 200.0

    subset = _run(ds, [])

    assert dict(subset.sizes) == {"TIME": 2, "I": 4, "J": 6}
    assert int(np.isfinite(subset.temp).sum()) == subset.temp.size


def _expected_inside(geometry, lats, lons) -> np.ndarray:
    """Oracle mask: shapely point-in-polygon per cell, in (lat, lon) order.

    Written as an explicit loop, not vectorised, so it does not lean on the same
    call the implementation uses.
    """
    return np.array(
        [[geometry.intersects(ShapelyPoint(lon, lat)) for lon in lons] for lat in lats]
    )


# A right triangle over the lower-left of the grid. Its hypotenuse runs exactly
# through the cell centres (0,4), (2,2), (4,0), so it also covers "cells ON the
# boundary are kept". Bbox = lat/lon [0, 4], so the crop must include the upper
# right corner cells that the triangle does not cover.
TRIANGLE = ShapelyPolygon([(0, 0), (4, 0), (0, 4)])


def test_polygon_shape_blanks_cells_only_the_bbox_asked_for():
    # The whole point of the geometry mask: the crop is a rectangle (all the zarr
    # library can slice), so the cells outside the drawn shape must come back NaN
    # while the shape's own cells - boundary included - keep their data.
    ds = _regular_grid()

    subset = _run(ds, [bbox_of(TRIANGLE)], geometry=TRIANGLE)

    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 5, "LONGITUDE": 5}
    kept = np.isfinite(subset.sst.isel(TIME=0).values)
    np.testing.assert_array_equal(kept, _expected_inside(TRIANGLE, range(5), range(5)))
    # spot checks: inside, on the hypotenuse, and outside it
    t0 = subset.sst.isel(TIME=0)
    assert np.isfinite(t0.sel(LATITUDE=1, LONGITUDE=1))
    assert np.isfinite(t0.sel(LATITUDE=2, LONGITUDE=2)), "boundary cell dropped"
    assert np.isnan(t0.sel(LATITUDE=4, LONGITUDE=4))
    # the data that survived is the store's own, not shifted or re-gridded
    assert t0.sel(LATITUDE=1, LONGITUDE=1) == ds.sst.isel(
        TIME=0, LATITUDE=1, LONGITUDE=1
    )


def test_polygon_hole_is_blanked():
    # A polygon with a hole (the user cut a piece out) - the hole's cells are
    # outside the shape, so they must be NaN.
    ds = _regular_grid()
    with_hole = ShapelyPolygon(
        [(0, 0), (6, 0), (6, 6), (0, 6)], [[(2, 2), (4, 2), (4, 4), (2, 4)]]
    )

    subset = _run(ds, [bbox_of(with_hole)], geometry=with_hole)

    t0 = subset.sst.isel(TIME=0)
    assert np.isnan(t0.sel(LATITUDE=3, LONGITUDE=3)), "hole not blanked"
    assert np.isfinite(t0.sel(LATITUDE=2, LONGITUDE=2)), "hole edge is still inside"
    assert np.isfinite(t0.sel(LATITUDE=1, LONGITUDE=5))


def test_rectangular_polygon_is_cropped_not_masked():
    # The common portal case: a drawn rectangle IS its own bbox, so the crop
    # already selects exactly those cells. No .where() means no NaN and no dtype
    # promotion - the download stays byte-for-byte what it was.
    ds = _regular_grid()
    rectangle = shapely_box(0, 0, 2, 2)

    subset = _run(ds, [bbox_of(rectangle)], geometry=rectangle)
    crop_only = _run(ds, [bbox_of(rectangle)])

    assert subset.sst.dtype == ds.sst.dtype
    assert not np.isnan(subset.sst.values).any()
    np.testing.assert_array_equal(subset.sst.values, crop_only.sst.values)


def test_disjoint_polygons_keep_both_shapes_and_blank_the_cross():
    # Two separate drawn areas: both keep their own data, and the cells the union
    # grid had to include (box 1's lats x box 2's lons) are NaN.
    ds = _regular_grid()
    low = shapely_box(0, 0, 2, 2)
    high = shapely_box(7, 7, 9, 9)
    geometry = ShapelyMultiPolygon([low, high])

    subset = _run(ds, [bbox_of(low), bbox_of(high)], geometry=geometry)

    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 6, "LONGITUDE": 6}
    t0 = subset.sst.isel(TIME=0)
    assert not np.isnan(t0.isel(LATITUDE=slice(0, 3), LONGITUDE=slice(0, 3))).any()
    assert not np.isnan(t0.isel(LATITUDE=slice(3, 6), LONGITUDE=slice(3, 6))).any()
    assert np.isnan(t0.isel(LATITUDE=slice(0, 3), LONGITUDE=slice(3, 6))).all()


def test_overlapping_polygons_are_one_shape_not_two_passes():
    # Overlap is dissolved before we get here (multi_polygon_helper), so the L
    # shape arrives as ONE geometry with ONE bbox: the crop covers the L's
    # envelope and the mask blanks the corner the L does not reach.
    ds = _regular_grid()
    l_shape = shapely_box(0, 0, 4, 2).union(shapely_box(0, 0, 2, 4))

    subset = _run(ds, [bbox_of(l_shape)], geometry=l_shape)

    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 5, "LONGITUDE": 5}
    t0 = subset.sst.isel(TIME=0)
    assert np.isfinite(t0.sel(LATITUDE=1, LONGITUDE=4)), "the wide arm of the L"
    assert np.isfinite(t0.sel(LATITUDE=4, LONGITUDE=1)), "the tall arm of the L"
    assert np.isnan(t0.sel(LATITUDE=4, LONGITUDE=4)), "the corner outside the L"


def test_curvilinear_grid_masked_by_polygon_shape():
    # 2D LATITUDE/LONGITUDE go through the same point-in-polygon mask, no special
    # case: the shape stays I x J and the cells outside the polygon are NaN.
    ds = _curvilinear_grid()

    subset = _run(ds, [bbox_of(TRIANGLE)], geometry=TRIANGLE)

    assert dict(subset.sizes) == {"TIME": 2, "I": 4, "J": 6}
    kept = np.isfinite(subset.temp.isel(TIME=0).values)
    expected = np.array(
        [
            [
                TRIANGLE.intersects(
                    ShapelyPoint(float(ds.LONGITUDE[i, j]), float(ds.LATITUDE[i, j]))
                )
                for j in range(ds.sizes["J"])
            ]
            for i in range(ds.sizes["I"])
        ]
    )
    np.testing.assert_array_equal(kept, expected)
    assert kept.any(), "the polygon covers part of this grid"


def test_trajectory_lat_lon_are_paired_not_meshed():
    # LATITUDE and LONGITUDE both run along TIME, so they are one point per step.
    # Meshing them would produce a (TIME, TIME) mask, which xarray cannot
    # broadcast - the download of vessel_satellite_radiance_delayed_qc.zarr fails
    # outright. Only the steps whose own point is inside the shape survive.
    ds = _trajectory()
    low_corner = shapely_box(0, 0, 2, 2)

    subset = _run(ds, [bbox_of(low_corner)], geometry=low_corner)

    assert dict(subset.sizes) == {"TIME": 2}
    kept = np.isfinite(subset.radiance.values)
    np.testing.assert_array_equal(kept, [True, True])


def test_trajectory_steps_outside_the_shape_are_blanked():
    # Same track (the time range keeps its first two steps), with a shape that
    # covers only step 0. Step 1 cannot be cropped away - lat/lon are not
    # dimensions - so it must come back NaN.
    ds = _trajectory()
    first_only = ShapelyPolygon([(-1, -1), (0.5, -1), (0.5, 0.5), (-1, 0.5)])

    subset = _run(ds, [bbox_of(first_only)], geometry=first_only)

    assert dict(subset.sizes) == {"TIME": 2}
    kept = np.isfinite(subset.radiance.values)
    np.testing.assert_array_equal(kept, [True, False])


def test_geometry_mask_keeps_shape_for_the_estimate():
    # The estimate skips the mask (eager .where() on a non-dask store -> OOM) and
    # relies on drop=False: masked and unmasked must agree on shape and nbytes.
    ds = _regular_grid()

    masked = _run(ds, [bbox_of(TRIANGLE)], geometry=TRIANGLE, apply_mask=True)
    unmasked = _run(ds, [bbox_of(TRIANGLE)], geometry=TRIANGLE, apply_mask=False)

    assert dict(unmasked.sizes) == dict(masked.sizes)
    assert int(unmasked.nbytes) == int(masked.nbytes)
    assert not np.isnan(unmasked.sst.values).any()


def test_geometry_mask_stays_lazy():
    # The mask reads the lat/lon axes only; the data variables must not compute.
    ds = _regular_grid().chunk({"TIME": 1})

    subset = _run(ds, [bbox_of(TRIANGLE)], geometry=TRIANGLE)

    assert isinstance(subset.sst.data, da.Array)


def test_empty_bboxes_slice_by_time_only():
    # No bbox means the user asked for no spatial filter: the time range is the
    # only filter, the lat/lon axes come back whole and untouched (no NaN).
    ds = _regular_grid()

    subset = _run(ds, [])

    assert dict(subset.sizes) == {"TIME": 2, "LATITUDE": 10, "LONGITUDE": 10}
    assert subset.sst.dtype == ds.sst.dtype
    np.testing.assert_array_equal(
        subset.sst.values,
        ds.sel(TIME=slice(START.tz_localize(None), END.tz_localize(None))).sst.values,
    )


def test_unknown_condition_name_raises():
    # A dataset without the resolved lat dim -> neither dim nor var -> ValueError.
    ds = xr.Dataset(
        {"v": ("TIME", np.zeros(4))},
        coords={"TIME": pd.date_range("2020-01-01", periods=4)},
    )
    with pytest.raises(ValueError, match="neither dim"):
        _run(ds, [BOX_LOW])
