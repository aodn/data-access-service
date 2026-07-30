"""Shared "apply the subset to a zarr store" behaviour.

The batch download (ZarrProcessor) and the size estimate must slice a zarr
dataset the SAME way, or their results drift.

Everything returned is a lazily-sliced xarray.Dataset: no chunk is loaded, so
callers can either write it out (download) or read .nbytes (estimate).
"""

from typing import Any, Iterable, Optional, Sequence

import numpy as np
import shapely
import xarray
from shapely.geometry import box as shapely_box
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from xarray import DataArray

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.date_time_utils import to_naive_utc
from data_access_service.utils.dim_names_utils import resolve_dim_names


def subset_zarr(
    dataset: xarray.Dataset,
    api,
    uuid: str,
    key: str,
    start_date,
    end_date,
    bboxes: Sequence[BoundingBox],
    apply_mask: bool = True,
    geometry: Optional[BaseGeometry] = None,
) -> xarray.Dataset:
    """Apply one time range + the requested area to a zarr dataset.

    Returns a lazily-sliced xarray.Dataset (no compute). Raises ValueError if a
    condition targets a name that is neither a dimension nor a variable of the
    dataset, or if bboxes is empty.

    Two steps, because the zarr library can only slice by value ranges:

    1. CROP: .isel() every lat/lon position covered by at least one bbox. With N
       bboxes that is the union grid - not the bounding envelope of all of them.
    2. MASK: .where() the cells the crop had to include but nobody asked for, so
       they come out as NaN: outside the drawn polygons when `geometry` is given
       (`geometry` is the merged shape, bboxes are only its bounding boxes), or
       outside the bboxes themselves when the crop could not express them
       exactly. See area_to_keep.

    :param bboxes: bounding boxes of the requested area, at least one
    :param geometry: the user's merged polygons (multi_polygon_helper), the shape
        the bboxes came from. None means "no polygon given" - see area_to_keep.
    :param apply_mask: when True (batch download) step 2 runs. When False (size
        estimation) it is skipped: on a non-dask store .where() is eager and
        would load the whole store from S3 (OOM), and with drop=False it never
        changes the shape/nbytes anyway.
    """
    if not bboxes:
        raise ValueError(
            f"subset_zarr needs at least one bbox (dataset: {key}). Callers pass "
            "ResolvedSubsetRequest.effective_bboxes, which defaults to the whole globe."
        )

    lat_name, lon_name, time_name = resolve_dim_names(api, uuid, key)
    conditions_per_bbox = [
        subset_conditions(lat_name, lon_name, time_name, start_date, end_date, bbox)
        for bbox in bboxes
    ]

    for dim_name in conditions_per_bbox[0]:
        if not is_dim(dim_name, dataset) and not is_var(dim_name, dataset):
            raise ValueError(
                f"Condition key: {dim_name} is neither dim, coord nor data_var in "
                f"the dataset. Dataset: {key}"
            )

    # Step 1: cut the store down to the positions ANY bbox asks for.
    dim_indexers = {
        dim_name: form_dim_indexer(
            dataset, dim_name, [c[dim_name] for c in conditions_per_bbox]
        )
        for dim_name in conditions_per_bbox[0]
        if is_dim(dim_name, dataset)
    }
    subset = dataset.isel(**dim_indexers) if dim_indexers else dataset

    # Step 2: blank whatever the crop could not express. Only built when it will
    # be used - it reads the lat/lon values, which the estimate path avoids.
    if apply_mask:
        area = area_to_keep(dataset, lat_name, lon_name, bboxes, geometry)
        if area is not None:
            # NOTE: KEEP drop=False
            # The size estimate SKIPS this .where() and relies on the
            # invariant that .where(drop=False) only promotes dtypes (mirrored via
            # maybe_promote in size_estimation._nbytes_by_compressibility), never
            # changes shape. Switching to drop=True would crop the grid and change nbytes,
            # silently making every estimate wrong. If you must change it, update the
            # estimation path to match.
            subset = subset.where(
                form_geometry_mask(subset, lat_name, lon_name, area), drop=False
            )
    return subset


def subset_conditions(
    lat_name: str,
    lon_name: str,
    time_name: str,
    start_date,
    end_date,
    bbox: BoundingBox,
) -> dict[str, list]:
    """Build {dim_name: [min, max]} for the time/lat/lon filters of one bbox.

    Add more conditions here if they become supported.
    """
    return {
        time_name: [to_naive_utc(start_date), to_naive_utc(end_date)],
        lat_name: [bbox.min_lat, bbox.max_lat],
        lon_name: [bbox.min_lon, bbox.max_lon],
    }


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
    """The area a cell must fall inside to survive, or None when the crop
    already selected exactly the requested cells and nothing needs blanking.

    Also answers "will the download NaN-fill anything?" for the size estimate,
    so the two cannot disagree about it.

    - the user's merged polygons when we have them: the crop only used their
      bounding boxes, so the shape itself (holes, diagonals, the corners of an
      L-shaped union) still has to be applied. A polygon that IS its own bbox
      needs no mask - the crop is already that rectangle, and skipping .where()
      keeps the data's own dtypes.
    - else the bboxes as rectangles: the crop could not express them exactly,
      either because lat/lon are 2D variables that cannot be indexed by value,
      or because several boxes leave "cross" cells in the union grid that belong
      to no box.
    - None when a single bbox was cropped by value: those cells ARE the request.
      This is also what keeps the whole-globe default (no polygon given) from
      masking at all - a -180..180 rectangle would blank every cell of a store
      whose longitudes run 0..360.
    """
    # one bbox on indexable (1D) lat/lon axes is the only case the crop states
    # exactly, cell for cell
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

    One point-in-polygon test covers both grid layouts, no special case:
      - regular grid: 1D lat and lon axes, meshed into the (lat, lon) plane
      - curvilinear grid: 2D LATITUDE/LONGITUDE variables sharing dims (I, J)

    The lat/lon values are read here (coordinate-sized, not data-sized); the data
    variables stay lazy.
    """
    lat = dataset[lat_name]
    lon = dataset[lon_name]

    if lat.ndim == 1 and lon.ndim == 1:
        # meshgrid's default indexing gives (lat, lon)-shaped planes
        lon_values, lat_values = np.meshgrid(lon.values, lat.values)
        dims = lat.dims + lon.dims
    elif lat.dims == lon.dims:
        lat_values, lon_values = lat.values, lon.values
        dims = lat.dims
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
