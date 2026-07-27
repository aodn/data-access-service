"""Shared "apply the subset to a zarr store" behaviour.

The batch download (ZarrProcessor) and the size estimate must slice a zarr
dataset the SAME way, or their results drift.

Everything returned is a lazily-sliced xarray.Dataset: no chunk is loaded, so
callers can either write it out (download) or read .nbytes (estimate).
"""

from typing import Any, Iterable, Sequence

import numpy as np
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
    bboxes: Sequence[BoundingBox],
    apply_mask: bool = True,
) -> xarray.Dataset:
    """Apply one time range + one or more bboxes to a zarr dataset.

    Returns a lazily-sliced xarray.Dataset (no compute). Raises ValueError if a
    condition targets a name that is neither a dimension nor a variable of the
    dataset, or if bboxes is empty.

    With N bboxes the result is the union grid: the smallest rectangular grid holding all the requested boxes - every lat/lon position covered
    by at least one bbox, with the cells that belong to no bbox set to NaN.

    apply_mask: when True (batch download) the .where() mask is applied - both
    the curvilinear 2D-variable conditions and the multi-bbox cross-cell
    blanking. When False (size estimation) the .where() is skipped: on a
    non-dask store it is eager and would load the whole store from S3 (OOM), and
    it never changes the shape/nbytes anyway (drop=False).
    """
    if not bboxes:
        raise ValueError(
            f"subset_zarr needs at least one bbox (dataset: {key}). Callers pass "
            "ResolvedSubsetRequest.effective_bboxes, which defaults to the whole globe."
        )

    conditions_per_bbox = [
        subset_conditions(api, uuid, key, start_date, end_date, bbox) for bbox in bboxes
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

    # Step 2: one mask term per bbox, OR'd together, so a cell survives when it
    # is inside ANY bbox. Built on the already-indexed `subset` so mask and data
    # share the same axes. Two kinds of condition end up here:
    #   - dimensions whose range differs between bboxes (lat/lon): the union
    #     indexer above keeps the "cross" cells that no single bbox asked for,
    #     and this blanks them. A dimension asking for the same range in every
    #     bbox (time) is already exact, so it is skipped.
    #   - N-dimensional variables, especially curvilinear 2D lat/lon, which
    #     cannot be indexed by value at all.
    mask: DataArray | None = None
    for conditions in conditions_per_bbox:
        bbox_mask: DataArray | None = None
        for dim_name, (min_value, max_value) in conditions.items():
            if is_dim(dim_name, dataset) and not varies_between_bboxes(
                dim_name, conditions_per_bbox
            ):
                continue
            bbox_mask = form_mask(bbox_mask, dim_name, min_value, max_value, subset)
        if bbox_mask is not None:
            mask = bbox_mask if mask is None else (mask | bbox_mask)

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


def varies_between_bboxes(
    dim_name: str, conditions_per_bbox: Sequence[dict[str, list]]
) -> bool:
    """True when the bboxes ask for different ranges on this dimension.

    The time range is the same for every bbox, the lat/lon ranges usually are
    not. Only a varying dimension can produce "cross" cells that the union
    indexer picks up but no bbox actually asked for.
    """
    first = conditions_per_bbox[0][dim_name]
    return any(conditions[dim_name] != first for conditions in conditions_per_bbox[1:])


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
