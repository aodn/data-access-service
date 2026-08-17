"""Shared "apply the subset to a zarr store" behaviour.

The batch download (ZarrProcessor) and the size estimate must slice a zarr
dataset the SAME way, or their results drift.

Everything returned is a lazily-sliced xarray.Dataset: no chunk is loaded, so
callers can either write it out (download) or read .nbytes (estimate).
"""

from typing import Any, Iterable, Optional, Sequence

import numpy as np
from pandas import Timestamp
import shapely
import xarray
from shapely.geometry import box as shapely_box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from xarray import DataArray

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.date_time_utils import to_naive_utc


def subset_zarr(
    dataset: xarray.Dataset,
    key: str,
    lat_name: str,
    lon_name: str,
    time_name: str,
    start_date: Timestamp,
    end_date: Timestamp,
    bboxes: Sequence[BoundingBox],
    apply_mask: bool = True,
    geometry: Optional[BaseGeometry] = None,
) -> xarray.Dataset:
    """Apply one time range + the requested area to a zarr dataset.

    Returns a lazily-sliced xarray.Dataset.

    1. CROP: .isel() every lat/lon position covered by at least one bbox. With N
       bboxes that is the union of their grids, not one envelope around them.
       With no bbox the time range is cropped and lat/lon are left alone.
    2. MASK: .where() the cells the crop had to include but nobody asked for, so
       they come out as NaN. See area_to_keep for which area that is.

    :param key: dataset key, used in error messages only
    :param lat_name/lon_name/time_name: the dataset's own dimension names, from
        api.resolve_dim_names(uuid, key)
    :param start_date/end_date: both required - None is not an "open end"
    :param bboxes: the requested area. Empty means the user asked for no spatial
        filter, so the time range is the only filter: the whole grid is kept,
        untouched (no crop, no mask).
    :param geometry: the merged polygons the bboxes came from, None if the user
        drew no polygon
    :param apply_mask: False for the size estimate only: on a non-dask store
        .where() is eager and would load the whole store from S3 (OOM), and with
        drop=False it cannot change nbytes anyway.

    Raises ValueError if a condition names something that is neither a dimension
    nor a variable of the dataset.
    """
    # Step 1: build the conditions for each bbox, then check they are all valid.
    # No bbox means no spatial filter, so time is the only condition.
    if bboxes:
        conditions_per_bbox = [
            subset_conditions(lat_name, lon_name, time_name, start_date, end_date, bbox)
            for bbox in bboxes
        ]
    else:
        conditions_per_bbox = [time_condition(time_name, start_date, end_date)]

    for dim_name in conditions_per_bbox[0]:
        if not is_dim(dim_name, dataset) and not is_var(dim_name, dataset):
            raise ValueError(
                f"Condition key: {dim_name} is neither dim, coord nor data_var in "
                f"the dataset. Dataset: {key}"
            )

    # Step 2: crop the zarr store down to the positions which fall inside at least one bbox
    dim_indexers = {
        dim_name: form_dim_indexer(
            dataset, dim_name, [c[dim_name] for c in conditions_per_bbox]
        )
        for dim_name in conditions_per_bbox[0]
        if is_dim(dim_name, dataset)
    }
    subset = dataset.isel(**dim_indexers) if dim_indexers else dataset

    # Step 3: mask the cells that fall outside the requested area
    if apply_mask:
        mask = area_mask(subset, lat_name, lon_name, bboxes, geometry)
        if mask is not None:
            # NOTE: KEEP drop=False
            # The size estimate SKIPS this .where() and relies on the
            # invariant that .where(drop=False). Switching to drop=True would
            # silently making estimate wrong. If you must change it, update
            # the estimation path to match.
            subset = subset.where(mask, drop=False)
    return subset


def area_mask(
    dataset: xarray.Dataset,
    lat_name: str,
    lon_name: str,
    bboxes: Sequence[BoundingBox],
    geometry: Optional[BaseGeometry],
) -> Optional[DataArray]:
    """The .where() mask keeping only the requested area, None when no mask is
    needed."""
    area = area_to_keep(dataset, lat_name, lon_name, bboxes, geometry)
    if area is None:
        return None
    return form_geometry_mask(dataset, lat_name, lon_name, area)


def selects_no_data(
    subset: xarray.Dataset,
    lat_name: str,
    lon_name: str,
    time_name: str,
    bboxes: Sequence[BoundingBox],
    geometry: Optional[BaseGeometry] = None,
) -> bool:
    """True when a subset_zarr() result would write out an empty or all-NaN file.

    Two checks, because the crop only cuts 1D axes:
    - a dimension came back empty
    - nothing is left inside the drawn area. A store with 2D lat/lon (or lat/lon
      along TIME) keeps its full grid even when the area misses it completely.

    Only coordinates are read - the data variables stay lazy.

    :param subset: the sliced dataset, not the whole store
    """
    if any(
        subset.sizes.get(dim_name) == 0 for dim_name in (time_name, lat_name, lon_name)
    ):
        return True
    mask = area_mask(subset, lat_name, lon_name, bboxes, geometry)
    return mask is not None and not bool(mask.any())


def subset_conditions(
    lat_name: str,
    lon_name: str,
    time_name: str,
    start_date: Timestamp,
    end_date: Timestamp,
    bbox: BoundingBox,
) -> dict[str, list]:
    """Build {dim_name: [min, max]} for the time/lat/lon filters of one bbox."""
    return {
        **time_condition(time_name, start_date, end_date),
        lat_name: [bbox.min_lat, bbox.max_lat],
        lon_name: [bbox.min_lon, bbox.max_lon],
    }


def time_condition(
    time_name: str, start_date: Timestamp, end_date: Timestamp
) -> dict[str, list]:
    """Build {time_name: [min, max]} - the whole condition set when the request
    has no bbox, so lat/lon are never filtered on."""
    return {time_name: [to_naive_utc(start_date), to_naive_utc(end_date)]}


def is_dim(key: str, dataset: xarray.Dataset) -> bool:
    return key in dataset.dims


def is_var(key: str, dataset: xarray.Dataset) -> bool:
    # both coords and data_vars are in variables
    return key in dataset.variables


def area_to_keep(
    dataset: xarray.Dataset,
    lat_name: str,
    lon_name: str,
    bboxes: Sequence[BoundingBox],
    geometry: Optional[BaseGeometry],
) -> Optional[BaseGeometry]:
    """The area to keep. Cells outside it are set to NaN.

    Returns one of three things:

    - The resolved geometry, if the user drew any.

    - The bboxes joined into one shape, if the crop could not pick them exactly.
      That happens when lat/lon are 2D and cannot be indexed by value, or when
      several boxes leave cells that belong to no box. Note the API always sends
      a geometry alongside its bboxes, so this branch is mostly for direct
      callers.

    - None, when no mask is needed. Three ways to get here: no bbox at all (the
      request has no spatial filter, so nothing may be blanked), one bbox on 1D
      lat/lon, or a drawn polygon that is itself a rectangle.
    """
    if not bboxes:
        return None
    exact_crop = (
        len(bboxes) == 1 and is_dim(lat_name, dataset) and is_dim(lon_name, dataset)
    )
    rectangles = [
        shapely_box(bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat)
        for bbox in bboxes
    ]
    if geometry is not None:
        if exact_crop and geometry.equals(rectangles[0]):
            return None
        return geometry
    if exact_crop:
        return None
    return unary_union(rectangles)


def form_geometry_mask(
    dataset: xarray.Dataset, lat_name: str, lon_name: str, area: BaseGeometry
) -> DataArray:
    """True where a cell's lat/lon lies inside `area`, boundary included.

    One point-in-polygon test covers every layout the stores use, no special case:
      - lat/lon on the SAME dims: paired position by position. Covers a
        curvilinear grid (2D on (I, J)) and a trajectory (both 1D along TIME,
        where each time step is one point, not a row x column plane).
      - lat/lon on DIFFERENT 1D axes: a regular grid, meshed into the (lat, lon)
        plane.

    The lat/lon values are read here (coordinate-sized, not data-sized); the data
    variables stay lazy.
    """
    lat = dataset[lat_name]
    lon = dataset[lon_name]

    if lat.dims == lon.dims:
        lat_values, lon_values = lat.values, lon.values
        dims = lat.dims
    elif lat.ndim == 1 and lon.ndim == 1:
        # meshgrid's default indexing gives (lat, lon)-shaped planes
        lon_values, lat_values = np.meshgrid(lon.values, lat.values)
        dims = lat.dims + lon.dims
    else:
        raise ValueError(
            f"Cannot mask by area: {lat_name} {lat.dims} and {lon_name} {lon.dims} "
            "are neither 1D axes nor variables on the same dims"
        )

    # cells whose lat/lon is NaN (curvilinear fill values) fall outside anything
    inside = shapely.intersects_xy(area, lon_values, lat_values)
    return xarray.DataArray(inside, dims=dims)


def form_dim_indexer(
    dataset: xarray.Dataset,
    dim_name: str,
    ranges: Iterable[Sequence[Any]],
) -> Any:
    """Positions on `dim_name` covered by ANY of the ranges, as an .isel() indexer.

    Value comparison rather than .sel(slice(...)) so the axis direction does not
    matter (a descending lat axis needs the slice reversed, which is easy to get
    wrong) and so several ranges can be OR'd together.

    A contiguous run comes back as a plain slice - the single-bbox case then
    indexes exactly the same block of the store it always did, with no fancy
    indexing in the way.
    """
    values = dataset[dim_name].values
    selected = np.zeros(values.shape, dtype=bool)
    for min_value, max_value in ranges:
        selected |= (values >= min_value) & (values <= max_value)

    positions = np.nonzero(selected)[0]
    if positions.size == 0:
        return slice(0, 0)
    if positions[-1] - positions[0] + 1 == positions.size:
        return slice(int(positions[0]), int(positions[-1]) + 1)
    return positions
