"""Download size estimation workers.

API.estimate_datasets_size (core/api.py) resolves the request via
resolve_subset_request, then calls estimate_single_key_size here for each key.

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
    OUTPUT_FORMAT_COMPRESSION_RATIO,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
    GEOTIFF_CURVILINEAR_INFLATION,
)
from data_access_service.utils.dim_names_utils import resolve_dim_names
from data_access_service.utils.geotiff_export import geotiff_eligible_vars, has_ij_dims
from data_access_service.utils.subset_request_resolver import (
    ResolvedSubsetRequest,
    trim_date_range_for_keys,
)

log = logging.getLogger(__name__)


def estimate_single_key_size(
    api,
    key: str,
    resolved_subset_request: ResolvedSubsetRequest,
    output_format: str,
) -> Optional[dict]:
    """
    Estimate the download size of ONE key.

    :param key: Dataset key
    :param resolved_subset_request: The resolved subset request (dates already defaulted
        and trimmed to the union extent of all keys, "*" expanded, bboxes
        parsed); this function re-trims the dates to THIS key's own extent
    :param output_format: One of SUPPORTED_OUTPUT_FORMATS (netcdf/geotiff/csv)
    :return: A dict with the estimate, or None if the key is not found
    """
    uuid = resolved_subset_request.uuid
    ds = api.get_datasource(uuid, key)
    if ds is None:
        return None

    if not resolved_subset_request.has_data:
        return _empty_estimate(uuid, key, output_format)

    # Re-trim to THIS key's own extent
    date_start, date_end = trim_date_range_for_keys(
        api,
        uuid,
        [key],
        resolved_subset_request.start_date,
        resolved_subset_request.end_date,
    )
    if date_start is None or date_end is None:
        return _empty_estimate(uuid, key, output_format)

    if isinstance(ds, ZarrDataSource):
        # effective_bboxes is shared with the batch download: empty -> whole
        # globe, lons used raw (no [0, 360] shift). ds.zarr_store is the raw
        # lazy dataset (opened in ZarrDataSource.__init__)
        return _estimate_zarr_size(
            api,
            ds.zarr_store,
            uuid,
            key,
            date_start,
            date_end,
            resolved_subset_request.effective_bboxes,
            resolved_subset_request.columns,
            output_format,
        )
    elif isinstance(ds, ParquetDataSource):
        raise NotImplementedError("Parquet size estimate is not implemented yet")
    else:
        return None


def _estimate_zarr_size(
    api,
    zarr_store: xarray.Dataset,
    uuid: str,
    key: str,
    date_start: pd.Timestamp | None,
    date_end: pd.Timestamp | None,
    bboxes: list,
    columns: list[str] | None,
    output_format: str,
) -> dict:
    """Estimate the download size of one zarr key, summed over its bboxes.

    Overlapping bboxes are summed (counted once per box), so a multi-polygon
    estimate is an upper bound.

    :param zarr_store: the raw, unsliced lazy dataset for this key
    :param bboxes: effective bboxes to slice
    :param columns: requested columns; currently ignored
    :param output_format: "netcdf" or "geotiff"
    :return: dict with uuid, key, format, estimated_uncompressed_bytes,
        estimated_output_bytes, and human-readable notes
    """
    from data_access_service.utils.subset_zarr_helper import subset_zarr

    notes: list[str] = []
    if len(bboxes) > 1:
        notes.append(
            f"summed {len(bboxes)} polygon bboxes "
            "(overlapping regions counted once per box -> upper bound)"
        )
    if columns:
        # Column subsetting isn't implemented yet; once it is, it will be applied
        # in subset_zarr (shared with the download).
        # For now the estimate covers ALL columns
        log.info("column subsetting not implemented yet; ignoring columns %s", columns)
        notes.append(f"column subsetting not supported yet; columns skipped: {columns}")

    total_uncompressed = 0
    total_output = 0

    log.debug(
        "_estimate_zarr_size: uuid=%s key=%s slice=[%s..%s] bboxes=%d format=%s",
        uuid,
        key,
        date_start,
        date_end,
        len(bboxes),
        output_format,
    )

    for bbox in bboxes:
        # subset_zarr returns a lazily-sliced xarray.Dataset - the
        # SAME slicing the batch download uses.
        dataset: xarray.Dataset = subset_zarr(
            zarr_store, api, uuid, key, date_start, date_end, bbox
        )

        # Measure the uncompressed and output sizes for this slice, per format.
        if output_format == "geotiff":
            uncompressed, output = _measure_geotiff(api, dataset, uuid, key, notes)
        elif output_format == "netcdf":
            uncompressed, output = _measure_netcdf(dataset, output_format)
        total_uncompressed += uncompressed
        total_output += output

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


def _measure_netcdf(dataset: xarray.Dataset, output_format: str) -> tuple[int, int]:
    """(uncompressed, output) for netcdf: nbytes, then a flat compression
    ratio"""
    nbytes = int(dataset.nbytes)
    ratio = OUTPUT_FORMAT_COMPRESSION_RATIO[output_format]
    return nbytes, int(nbytes * ratio)


def _measure_geotiff(
    api,
    dataset: xarray.Dataset,
    uuid: str,
    key: str,
    notes: list[str],
) -> tuple[int, int]:
    """(uncompressed, output) for GeoTIFF.

    GeoTIFF export writes one .tif per eligible gridded variable - only numeric
    variables that have BOTH the lat and lon dimensions are exported; everything
    else is dropped.
    We see dataset with dim I/J as a curvilinear grid, else lat/lon as a regular grid.
    The exporter warps curvilinear grids to a regular lat/lon grid before writing,
    so the real raster is larger than the raw I x J cell count. A size estimate can't
    know the warped dimensions without reprojecting, so it multiplies the I x J
    estimate by a conservative inflation factor (GEOTIFF_CURVILINEAR_INFLATION).

        uncompressed ~= sum_over_vars(n_time x lat x lon x bytes_per_pixel)
            if curvilinear, uncompressed *= GEOTIFF_CURVILINEAR_INFLATION
        output       ~= uncompressed x zip_ratio

    Raises ValueError when no gridded variable is found at all (genuinely
    non-gridded data, e.g. point/timeseries).
    """
    lat_name, lon_name, time_name = resolve_dim_names(api, uuid, key)

    # curvilinear grids index by integer I/J instead of lon/lat; the real
    # exporter warps I/J -> lat/lon before writing (geotiff_export.py). A size
    # estimate only needs the cell COUNT, so it uses I x J and applies a
    # conservative inflation factor for the warp below.
    is_curvilinear = has_ij_dims(dataset)
    lat_dim, lon_dim = ("I", "J") if is_curvilinear else (lat_name, lon_name)

    # Same var selection the exporter uses, so we count exactly what it writes.
    eligible = geotiff_eligible_vars(dataset, lat_name, lon_name)
    if not eligible:
        # The real export (build_geotiff_zip) raises here too and fails the whole
        # download, so we stop instead of promising a size for a file that can
        # never be produced.
        raise ValueError(
            f"GeoTIFF export not possible for {key}: no gridded numeric variables "
            "(a variable must be on both the lat and lon axes)."
        )

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

    if is_curvilinear:
        # The warped raster is bigger than I x J, so inflate by a mid-range
        # factor. Note: a REGULAR grid stored as I/J (which the exporter reduces
        # to an exact I x J) is over-estimated here - telling them apart needs
        # loading the lat/lon coords, which a metadata-only estimate avoids.
        raw_raster_bytes = int(raw_raster_bytes * GEOTIFF_CURVILINEAR_INFLATION)
        notes.append(
            "curvilinear grid: warped to regular lat/lon at export, "
            f"size inflated x{GEOTIFF_CURVILINEAR_INFLATION} (approximate)"
        )

    return raw_raster_bytes, int(raw_raster_bytes * GEOTIFF_ZIP_RATIO)


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
