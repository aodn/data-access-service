"""Shared "apply the subset to a zarr store" behaviour.

The batch download (ZarrProcessor) and the size estimate must slice a zarr
dataset the SAME way, or their results drift.

Everything returned is a lazily-sliced xarray.Dataset: no chunk is loaded, so
callers can either write it out (download) or read .nbytes (estimate).
"""

from typing import Any

import xarray
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
    bbox: BoundingBox,
    apply_mask: bool = True,
) -> xarray.Dataset:
    """Apply one time range + one bbox to a zarr dataset.

    Returns a lazily-sliced xarray.Dataset (no compute). Raises ValueError if a
    condition targets a name that is neither a dimension nor a variable of the
    dataset.

    apply_mask: when True (batch download) the curvilinear 2D-variable
    conditions are applied via .where(). When False (size estimation) the
    .where()is skipped: on a non-dask store it is eager and would load the
    whole store from S3 (OOM), and it never changes the shape/nbytes anyway
    (drop=False).
    """
    conditions = subset_conditions(api, uuid, key, start_date, end_date, bbox)

    dim_conditions: dict[str, slice] = {}  # for .sel() on indexed dimensions
    mask: DataArray | None = (
        None  # for .where() on N-dimensional variables (especially curvilinear lat/lon)
    )

    for dim_name, (min_value, max_value) in conditions.items():
        if is_dim(dim_name, dataset):
            form_dim_conditions(dim_conditions, dim_name, min_value, max_value, dataset)
        elif is_var(dim_name, dataset):
            mask = form_mask(mask, dim_name, min_value, max_value, dataset)
        else:
            raise ValueError(
                f"Condition key: {dim_name} is neither dim, coord nor data_var in "
                f"the dataset. Dataset: {key}"
            )

    subset = dataset
    if dim_conditions:
        subset = subset.sel(**dim_conditions)
    if mask is not None and apply_mask:
        # NOTE: KEEP drop=False
        # The size estimate SKIPS this .where() and relies on the
        # invariant that .where(drop=False) only promotes dtypes (mirrored via
        # maybe_promote in size_estimation._nbytes_by_compressibility), never
        # changes shape. Switching to drop=True would crop the grid and change nbytes,
        # silently making every estimate wrong. If you must change it, update the
        # estimation path to match.
        subset = subset.where(mask, drop=False)
    return subset


def subset_conditions(
    api, uuid: str, key: str, start_date, end_date, bbox: BoundingBox
) -> dict[str, list]:
    """Build {dim_name: [min, max]} for the time/lat/lon filters of one bbox.

    Dimension names are resolved from the dataset's own metadata. Add more
    conditions here if they become supported.
    """
    lat_dim, lon_dim, time_dim = resolve_dim_names(api, uuid, key)

    return {
        time_dim: [to_naive_utc(start_date), to_naive_utc(end_date)],
        lat_dim: [bbox.min_lat, bbox.max_lat],
        lon_dim: [bbox.min_lon, bbox.max_lon],
    }


def is_dim(key: str, dataset: xarray.Dataset) -> bool:
    return key in dataset.dims


def is_var(key: str, dataset: xarray.Dataset) -> bool:
    # both coords and data_vars are in variables
    return key in dataset.variables


def form_mask(
    existing_mask: DataArray | None,
    dim_name: str,
    min_value: Any,
    max_value: Any,
    dataset: xarray.Dataset,
) -> DataArray:
    var_mask = (dataset[dim_name] >= min_value) & (dataset[dim_name] <= max_value)
    if existing_mask is None:
        return var_mask
    return existing_mask & var_mask


def form_dim_conditions(
    existing_conditions: dict[str, slice] | None,
    dim_name: str,
    min_value: Any,
    max_value: Any,
    dataset: xarray.Dataset,
) -> dict[str, slice]:
    # slice direction must match the axis direction, or .sel() returns empty
    slice_from = min_value
    slice_to = max_value

    # if descending, swap
    if dataset[dim_name][0] > dataset[dim_name][-1]:
        slice_from = max_value
        slice_to = min_value

    dim_condition = {dim_name: slice(slice_from, slice_to)}
    if existing_conditions is None:
        return dim_condition
    existing_conditions.update(dim_condition)
    return existing_conditions
