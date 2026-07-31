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
from shapely.geometry.base import BaseGeometry
from xarray.core import dtypes as xr_dtypes

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource, ZarrDataSource

from data_access_service.core.constants import (
    OUTPUT_FORMAT_COMPRESSION_RATIO,
    ASSUMED_STRING_BYTES,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
    GEOTIFF_CURVILINEAR_INFLATION,
)
from data_access_service.utils.geotiff_export import geotiff_eligible_vars, has_ij_dims
from data_access_service.utils.subset_request_resolver import (
    ResolvedSubsetRequest,
    trim_date_range_for_keys,
)

log = logging.getLogger(__name__)


def _bytes_to_mb(num_bytes: int) -> float:
    """Bytes -> decimal MB (1 MB = 1,000,000 bytes), matching how browsers and
    download tools report file sizes. Rounded to 1 decimal place for readability."""
    return round(num_bytes / 1_000_000, 1)


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
            resolved_subset_request.geometry,
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
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    bboxes: list,
    columns: list[str] | None,
    output_format: str,
    geometry: BaseGeometry | None = None,
) -> dict:
    """Estimate the download size of one zarr key.

    All bboxes are applied in ONE subset_zarr call - the same union grid the
    batch download writes - so the estimate measures the download's region, not
    a per-bbox sum (which under-counted the NaN cells between disjoint boxes
    and double-counted overlaps).

    :param zarr_store: the raw, unsliced lazy dataset for this key
    :param bboxes: effective bboxes to slice
    :param columns: requested columns; currently ignored
    :param output_format: "netcdf" or "geotiff"
    :param geometry: the drawn area the bboxes came from; the download blanks the
        cells outside it, which is what makes the output figure an upper bound
    :return: dict with uuid, key, format, estimated_uncompressed_bytes,
        estimated_output_bytes, and human-readable notes
    """
    from data_access_service.utils.subset_zarr_helper import area_to_keep, subset_zarr

    notes: list[str] = []
    if len(bboxes) > 1:
        notes.append(f"union grid of {len(bboxes)} polygon bboxes")

    lat_name, lon_name, time_name = api.resolve_dim_names(uuid, key)
    # Whether the download's .where() will actually run. It decides both the note
    # and the dtype promotion in _nbytes_by_compressibility - when no mask runs,
    # an int var stays int and must NOT be sized as the promoted float64.
    will_mask = (
        area_to_keep(zarr_store, lat_name, lon_name, bboxes, geometry) is not None
    )
    if will_mask:
        notes.append(
            "cells outside the requested area come out as NaN (they compress to "
            "almost nothing in the real file -> output estimate is an upper bound)"
        )
    if columns:
        # Column subsetting isn't implemented yet; once it is, it will be applied
        # in subset_zarr (shared with the download).
        # For now the estimate covers ALL columns
        log.info("column subsetting not implemented yet; ignoring columns %s", columns)
        notes.append(f"column subsetting not supported yet; columns skipped: {columns}")

    log.debug(
        "_estimate_zarr_size: uuid=%s key=%s slice=[%s..%s] bboxes=%d format=%s",
        uuid,
        key,
        date_start,
        date_end,
        len(bboxes),
        output_format,
    )

    # subset_zarr returns a lazily-sliced xarray.Dataset - the SAME region the
    # batch download writes (all bboxes in one pass). apply_mask=False skips the
    # eager .where() (OOM on the non-dask store); the mask uses drop=False, so it
    # only NaN-fills cells, never changes the shape/nbytes.
    dataset: xarray.Dataset = subset_zarr(
        zarr_store,
        key,
        lat_name,
        lon_name,
        time_name,
        date_start,
        date_end,
        bboxes,
        apply_mask=False,
        geometry=geometry,
    )

    # Measure the uncompressed and output sizes of the union grid, per format.
    if output_format == "geotiff":
        total_uncompressed, total_output = _measure_geotiff(
            api, dataset, uuid, key, notes
        )
    elif output_format == "netcdf":
        total_uncompressed, total_output = _measure_netcdf(
            dataset, output_format, notes, will_mask
        )

    # Human-readable size summary (applies to every output format).
    notes.append(
        f"estimated download size ~{_bytes_to_mb(total_output)} MB "
        f"(uncompressed ~{_bytes_to_mb(total_uncompressed)} MB)"
    )

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


def _measure_netcdf(
    dataset: xarray.Dataset, output_format: str, notes: list[str], will_mask: bool
) -> tuple[int, int]:
    """(uncompressed, output) for netcdf.

    Piecewise compression: the download zlib-compresses only numeric data
    variables, so the ratio applies to those bytes alone; coords and
    string/object vars are written uncompressed and pass through at 1.0.
    Applying one flat ratio to the whole nbytes would over-count compression
    on the coords (which never shrink)."""
    compressible, incompressible = _nbytes_by_compressibility(dataset, will_mask)
    uncompressed = compressible + incompressible
    ratio = OUTPUT_FORMAT_COMPRESSION_RATIO[output_format]
    output = int(compressible * ratio) + incompressible
    notes.append(
        f"netcdf: numeric data vars x compression ratio {ratio} + "
        "coords/string vars uncompressed "
        "(ratio calibrated on gridded SST; noisier data may compress less)"
    )
    return uncompressed, output


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
    lat_name, lon_name, time_name = api.resolve_dim_names(uuid, key)

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


def _estimated_string_width(dtype) -> int:
    """
    The download writes strings as fixed-width S{N} in convert_object_dtype_variables.

    A fixed-width numpy string ('S'/'U') already carries its real byte width; an
    object dtype only carries an 8-byte pointer (the string body lives on the heap), so we fall back to a nominal width.
    """
    if dtype.kind in {"S", "U"} and dtype.itemsize > 0:
        return dtype.itemsize
    return ASSUMED_STRING_BYTES


def _nbytes_by_compressibility(
    dataset: xarray.Dataset, will_mask: bool
) -> tuple[int, int]:
    """
    The download zlib-compresses only numeric data vars (dtype.kind in {i, u, f});
    coords and string/object vars are written uncompressed.

    `will_mask` says whether the download's .where() actually runs (i.e. whether
    area_to_keep returned an area). Only then does it NaN-fill, which promotes
    int/uint vars to float64 - so only then may we size them as promoted. Sizing
    an unmasked int16 var as float64 over-estimates it 4x.

    String-like data vars promote to object (8-byte pointer) under maybe_promote,
    but the download stores them as fixed-width S{N}, so we size them by
    _estimated_string_width instead of the 8-byte pointer.
    """
    compressible = 0
    incompressible = 0
    for name, var in dataset.variables.items():
        if name in dataset.data_vars:
            dtype = var.dtype
            if will_mask:
                dtype, _ = xr_dtypes.maybe_promote(dtype)
            if dtype.kind in {"i", "u", "f"}:
                compressible += int(var.size) * dtype.itemsize
            else:
                # object/string/bool -> stored as fixed-width S{N}, uncompressed
                incompressible += int(var.size) * _estimated_string_width(var.dtype)
        else:
            incompressible += int(var.nbytes)
    return compressible, incompressible


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
