"""Download size estimation workers.

API.estimate_datasets_size (core/api.py) resolves the request via
resolve_subset, then calls estimate_single_key_size here for each key.

The workers only measure metadata, never load the data:

- zarr (netcdf/csv output): xarray .nbytes of the lazy selection, scaled by
  a per-format compression ratio
- zarr (geotiff output): dimension-based - one raster per (gridded variable
  x time step), sized lat x lon, scaled by a zip ratio
- parquet: not implemented yet (raises NotImplementedError)
"""

import logging
from typing import Optional

import pandas as pd
import xarray

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource, ZarrDataSource

from data_access_service.core.constants import (
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
    OUTPUT_FORMAT_COMPRESSION_RATIO,
    COMPRESSION_RATIO_GEOTIFF,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
)
from data_access_service.utils.subsetting_resolver import (
    ResolvedSubset,
    trim_date_range_for_keys,
)

log = logging.getLogger(__name__)


def estimate_single_key_size(
    api,
    key: str,
    resolved_subset: ResolvedSubset,
    output_format: str,
) -> Optional[dict]:
    """
    Estimate the download size of ONE key.

    :param key: Dataset key
    :param resolved_subset: The resolved subset request (dates already defaulted
        and trimmed to the union extent of all keys, "*" expanded, bboxes
        parsed); this function re-trims the dates to THIS key's own extent
    :param output_format: One of SUPPORTED_OUTPUT_FORMATS (netcdf/geotiff/csv)
    :return: A dict with the estimate, or None if the key is not found
    """
    uuid = resolved_subset.uuid
    ds = api.get_datasource(uuid, key)
    if ds is None:
        return None

    if not resolved_subset.has_data:
        return _empty_estimate(uuid, key, output_format)

    # Re-trim to THIS key's own extent
    date_start, date_end = trim_date_range_for_keys(
        api, uuid, [key], resolved_subset.start_date, resolved_subset.end_date
    )
    if date_start is None or date_end is None:
        return _empty_estimate(uuid, key, output_format)

    # One slice per bbox. effective_bboxes is shared with the batch download
    # (subset_zarr), so both select the same region: empty -> whole globe,
    # and lons are used raw - no [0, 360] shift.
    spatial_slices = [
        (b.min_lat, b.max_lat, b.min_lon, b.max_lon)
        for b in resolved_subset.effective_bboxes
    ]

    if isinstance(ds, ZarrDataSource):
        return _estimate_zarr_size(
            api,
            ds,
            uuid,
            key,
            date_start,
            date_end,
            spatial_slices,
            resolved_subset.columns,
            output_format,
        )
    elif isinstance(ds, ParquetDataSource):
        raise NotImplementedError("Parquet size estimate is not implemented yet")
    else:
        return None


def _estimate_zarr_size(
    api,
    ds: ZarrDataSource,
    uuid: str,
    key: str,
    date_start: pd.Timestamp | None,
    date_end: pd.Timestamp | None,
    spatial_slices: list[tuple],
    columns: list[str] | None,
    output_format: str,
) -> dict:
    date_start_str = _timestamp_to_zarr_slice_str(date_start)
    date_end_str = _timestamp_to_zarr_slice_str(date_end)

    notes: list[str] = []
    if len(spatial_slices) > 1:
        notes.append(
            f"summed {len(spatial_slices)} polygon bboxes "
            "(overlapping regions counted once per box -> upper bound)"
        )

    total_uncompressed = 0
    total_output = 0
    columns_note_done = False

    log.debug(
        "_estimate_zarr_size: uuid=%s key=%s slice=[%s..%s] bboxes=%d format=%s",
        uuid,
        key,
        date_start_str,
        date_end_str,
        len(spatial_slices),
        output_format,
    )

    for la_min, la_max, lo_min, lo_max in spatial_slices:
        # get_data returns a lazily-sliced xarray.Dataset; chunks are NOT
        # loaded. We measure this one bbox and discard it before the next,
        # so no union grid is ever materialised.
        dataset: xarray.Dataset = ds.get_data(
            date_start_str, date_end_str, la_min, la_max, lo_min, lo_max
        )

        # Restrict to requested data variables, if any (reduces output size).
        # Columns subsetting not implemented yet but we want to account for it in the estimate
        if columns:
            mapped = api.map_column_names(uuid, key, columns) or []
            present = [c for c in mapped if c in dataset.data_vars]
            missing = [c for c in columns if c not in present]
            if present:
                dataset = dataset[present]
            if missing and not columns_note_done:
                notes.append(f"columns not found and ignored: {missing}")
        columns_note_done = True

        # .nbytes is metadata only (no compute); sum across bboxes.
        total_uncompressed += int(dataset.nbytes)

        # Output size depends on format - geotiff is NOT a flat multiplier on
        # nbytes so it has its own dimension-based estimate.
        if output_format == "geotiff":
            total_output += _estimate_geotiff_output_bytes(
                api, dataset, uuid, key, notes
            )
        else:
            ratio = OUTPUT_FORMAT_COMPRESSION_RATIO[output_format]
            total_output += int(int(dataset.nbytes) * ratio)

    if output_format != "geotiff":
        ratio = OUTPUT_FORMAT_COMPRESSION_RATIO[output_format]
        notes.append(f"compression ratio assumed: {ratio} for {output_format}")

    deduped_notes = list(dict.fromkeys(notes))

    log.debug(
        "_estimate_zarr_size: totals uncompressed=%d output=%d",
        total_uncompressed,
        total_output,
    )

    return {
        "uuid": uuid,
        "key": key,
        "format": output_format,
        "estimated_uncompressed_bytes": total_uncompressed,
        "estimated_output_bytes": total_output,
        "notes": "; ".join(deduped_notes),
    }


def _empty_estimate(uuid: str, key: str, output_format: str) -> dict:
    """Zero-size estimate, returned when the requested range is outside the
    dataset's temporal extent (the batch download produces no data here)."""
    return {
        "uuid": uuid,
        "key": key,
        "format": output_format,
        "estimated_uncompressed_bytes": 0,
        "estimated_output_bytes": 0,
        "notes": "requested date range is outside the dataset's temporal extent",
    }


def _estimate_geotiff_output_bytes(
    api,
    dataset: xarray.Dataset,
    uuid: str,
    key: str,
    notes: list[str],
) -> int:
    """
    Estimate GeoTIFF download size for a zarr selection

    GeoTIFF is not a flat ratio on nbytes. The real export writes one .tif per (eligible gridded variable x time step), each raster sized lat_size x lon_size, then bundles them into a ZIP. Only numeric variables that have BOTH the lat and
    lon dimensions are exported; everything else is dropped.

    estimated_output_bytes ~= sum_over_vars(n_time x lat x lon x bytes_per_pixel) x zip_ratio

    Falls back to the flat COMPRESSION_RATIO_GEOTIFF on nbytes only when no
    gridded variable is found at all (genuinely non-gridded data, e.g.
    point/timeseries).
    """
    lat_mapped = api.map_column_names(uuid, key, [STR_LATITUDE_UPPER_CASE]) or []
    lon_mapped = api.map_column_names(uuid, key, [STR_LONGITUDE_UPPER_CASE]) or []
    time_mapped = api.map_column_names(uuid, key, [STR_TIME_UPPER_CASE]) or []
    lat_name = lat_mapped[0] if lat_mapped else None
    lon_name = lon_mapped[0] if lon_mapped else None
    time_name = time_mapped[0] if time_mapped else None

    # curvilinear grids index by integer I/J instead of lon/lat. The real
    # exporter remaps I/J -> lat/lon before writing (subset_zarr.py).
    # But a size estimate only needs the cell COUNT so we don't do
    # conversion here.
    if "I" in dataset.dims and "J" in dataset.dims:
        lat_dim, lon_dim = "I", "J"
    else:
        lat_dim, lon_dim = lat_name, lon_name

    # Eligible = numeric variables gridded on both spatial dims, which
    # aligns with the exporter logic.
    eligible = [
        v
        for v in dataset.data_vars
        if v not in (lat_name, lon_name)
        and dataset[v].dtype.kind in ("i", "u", "f")
        and lat_dim in dataset[v].dims
        and lon_dim in dataset[v].dims
    ]
    if not eligible:
        notes.append(
            "geotiff: no gridded variables found, "
            f"fell back to flat ratio {COMPRESSION_RATIO_GEOTIFF}"
        )
        return int(dataset.nbytes * COMPRESSION_RATIO_GEOTIFF)

    lat_size = int(dataset.sizes.get(lat_dim, 1))
    lon_size = int(dataset.sizes.get(lon_dim, 1))
    n_time = int(dataset.sizes.get(time_name, 1)) if time_name else 1

    raw_raster_bytes = 0
    for v in eligible:
        # Integer rasters are cast to float32 before writing; floats keep
        # their own itemsize.
        kind = dataset[v].dtype.kind
        bytes_per_pixel = (
            GEOTIFF_INT_PIXEL_BYTES if kind in ("i", "u") else dataset[v].dtype.itemsize
        )
        raw_raster_bytes += n_time * lat_size * lon_size * bytes_per_pixel

    notes.append(
        f"geotiff: {len(eligible)} gridded var(s) x {n_time} time step(s), "
        f"grid {lat_size}x{lon_size}, zip ratio {GEOTIFF_ZIP_RATIO}"
    )
    return int(raw_raster_bytes * GEOTIFF_ZIP_RATIO)


def _timestamp_to_zarr_slice_str(ts: pd.Timestamp | None) -> str | None:
    """
    Convert a pandas Timestamp into the plain date string ZarrDataSource expects.

    ZarrDataSource.get_data slices the time coordinate with a string and cannot compare timezone-aware
    values, so we drop the tz (the values are already UTC by convention). Returns None unchanged
    so the slice stays open.
    """
    # lazy import: date_time_utils imports core.api at module level, which
    # imports this module - a top-level import here would be circular
    from data_access_service.utils.date_time_utils import to_naive_utc

    ts = to_naive_utc(ts)
    return None if ts is None else ts.strftime("%Y-%m-%d %H:%M:%S")
