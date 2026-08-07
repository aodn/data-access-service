"""Download size estimation workers.

API.estimate_datasets_size (core/api.py) resolves the request, then calls
estimate_single_key_size here for each key.

The workers only read metadata, never the data itself:

- zarr (netcdf): xarray .nbytes of the lazy selection x a compression ratio
- zarr (geotiff): one raster per (gridded variable x time step), sized
  lat x lon, x a zip ratio
- parquet: partition pruning + row-group statistics give a row count, x the
  CSV width of one row, x a zip ratio
"""

import logging
from datetime import datetime
from math import ceil
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pa_ds
import xarray
from shapely import wkb
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry
from xarray.core import dtypes as xr_dtypes

from aodn_cloud_optimised.lib.DataQuery import (
    ParquetDataSource,
    PolygonNotIntersectingError,
    ZarrDataSource,
    get_timestamps_boundary_values,
    query_unique_value,
)

from data_access_service.core.constants import (
    COMPRESSION_RATIO_CSV_GZIP,
    CSV_BYTES_PER_BOOL,
    CSV_BYTES_PER_DATE,
    CSV_BYTES_PER_FLOAT32,
    CSV_BYTES_PER_FLOAT64,
    CSV_BYTES_PER_INT,
    CSV_BYTES_PER_NULL,
    CSV_BYTES_PER_TIMESTAMP,
    CSV_SEPARATOR_BYTES,
    OUTPUT_FORMAT_COMPRESSION_RATIO,
    ASSUMED_STRING_BYTES,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
    GEOTIFF_CURVILINEAR_INFLATION,
    MAX_FRAGMENT_FOOTER_READS,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.date_time_utils import ensure_timezone
from data_access_service.utils.geotiff_export import geotiff_eligible_vars, has_ij_dims
from data_access_service.utils.subset_request_resolver import (
    ResolvedSubsetRequest,
    trim_date_range_for_keys,
)

log = logging.getLogger(__name__)


def _bytes_to_mb(num_bytes: int) -> float:
    """Bytes -> decimal MB (1 MB = 1,000,000 bytes), the way browsers report
    file sizes. Rounded to 1 decimal place."""
    return round(num_bytes / 1_000_000, 1)


def estimate_single_key_size(
    api,
    key: str,
    resolved_subset_request: ResolvedSubsetRequest,
    output_format: str,
) -> Optional[dict]:
    """
    Estimate the download size of ONE key.

    :param key: dataset key
    :param resolved_subset_request: the resolved request (dates defaulted, "*"
        expanded, bboxes parsed); this function re-trims the dates to THIS key's
        own extent
    :param output_format: one of SUPPORTED_OUTPUT_FORMATS (netcdf/geotiff/csv)
    :return: dict with the estimate, or None if the key is not found
    :raises ValueError: if the key can never produce output_format
    """
    uuid = resolved_subset_request.uuid
    ds = api.get_datasource(uuid, key)
    if ds is None:
        return None

    _check_format_supported(api, ds, uuid, key, output_format)

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
        return _estimate_zarr_size(
            api,
            ds.zarr_store,
            uuid,
            key,
            date_start,
            date_end,
            resolved_subset_request.bboxes,
            resolved_subset_request.columns,
            output_format,
            resolved_subset_request.geometry,
        )
    elif isinstance(ds, ParquetDataSource):
        return _estimate_parquet_size(
            api,
            ds,
            uuid,
            key,
            date_start,
            date_end,
            resolved_subset_request.bboxes,
            resolved_subset_request.columns,
            output_format,
        )
    else:
        return None


def _check_format_supported(api, ds, uuid: str, key: str, output_format: str) -> None:
    """Fail fast when this key can never produce output_format, before any
    metadata or slicing work is spent on it.

    The frontend only offers csv for parquet keys and netcdf/geotiff for zarr
    keys, so a mismatch is a malformed request, not something to estimate
    around. The deeper raises in _estimate_zarr_size and _measure_geotiff stay
    as backstops.

    :raises ValueError: on any key-format mismatch
    """
    if isinstance(ds, ParquetDataSource) and output_format != "csv":
        raise ValueError(
            f"'{output_format}' export not possible for {key}: a parquet key "
            "downloads as csv only."
        )
    if isinstance(ds, ZarrDataSource):
        if output_format == "csv":
            raise ValueError(
                f"'csv' export not possible for {key}: a zarr key downloads as "
                "netcdf or geotiff only."
            )
        if output_format == "geotiff":
            lat_name, lon_name, _ = api.resolve_dim_names(uuid, key)
            # Dims and dtypes never change under slicing, so the raw store
            # answers this without any subsetting work.
            if not geotiff_eligible_vars(ds.zarr_store, lat_name, lon_name):
                raise ValueError(
                    f"GeoTIFF export not possible for {key}: no gridded numeric "
                    "variables (a variable must be on both the lat and lon axes)."
                )


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

    All bboxes go through ONE subset_zarr call - the same union grid the batch
    download writes - so the estimate measures the download's region, not a
    per-bbox sum (which mis-counted the gaps and overlaps between boxes).

    :param zarr_store: the raw, unsliced lazy dataset for this key
    :param bboxes: bboxes to slice; empty means no spatial filter
    :param columns: requested columns; currently ignored
    :param output_format: "netcdf" or "geotiff"
    :param geometry: the drawn area the bboxes came from; the download blanks
        the cells outside it, which is why the output figure is an upper bound
    :return: dict with uuid, key, format, estimated_uncompressed_bytes,
        estimated_output_bytes and notes
    :raises ValueError: if a zarr key cannot download as output_format
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
    else:
        # "csv" is a valid request format, but only for parquet keys - the
        # download's zarr_processor.__format_handler has no csv handler and
        # raises the same way, so there is no size to promise here.
        raise ValueError(
            f"'{output_format}' export not possible for {key}: a zarr key "
            "downloads as netcdf or geotiff only."
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

    The download zlib-compresses only numeric data vars, so the ratio applies to
    those bytes alone; coords and string vars are written uncompressed. One flat
    ratio over the whole nbytes would over-count compression on the coords."""
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

    The export writes one .tif per eligible variable - numeric and on BOTH the
    lat and lon dims - for every time step:

        uncompressed ~= sum_over_vars(n_time x lat x lon x bytes_per_pixel)
            if curvilinear, uncompressed *= GEOTIFF_CURVILINEAR_INFLATION
        output       ~= uncompressed x zip_ratio

    A dataset on I/J dims is curvilinear. The exporter warps it to a regular
    lat/lon grid, so the real raster is bigger than I x J; the warped size is
    unknowable without reprojecting, hence the inflation factor.

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
    Byte width of one string value, as the download stores it (fixed-width S{N},
    see convert_object_dtype_variables).

    An 'S'/'U' dtype already carries its real width; an object dtype only
    carries an 8-byte pointer (the string body lives on the heap), so it falls
    back to a nominal width.
    """
    if dtype.kind in {"S", "U"} and dtype.itemsize > 0:
        return dtype.itemsize
    return ASSUMED_STRING_BYTES


def _nbytes_by_compressibility(
    dataset: xarray.Dataset, will_mask: bool
) -> tuple[int, int]:
    """
    (compressible, incompressible) nbytes: the download zlib-compresses only
    numeric data vars (kind i/u/f), everything else is written uncompressed.

    `will_mask` says whether the download's .where() actually runs. Only then
    does it NaN-fill, which promotes int vars to float64, so only then may we
    size them as promoted - sizing an unmasked int16 as float64 over-estimates
    it 4x.

    String data vars are sized by _estimated_string_width, not the 8-byte
    pointer maybe_promote would give, because the download stores them
    fixed-width.
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


def _estimate_parquet_size(
    api,
    parquet_ds: ParquetDataSource,
    uuid: str,
    key: str,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    bboxes: list[BoundingBox],
    columns: list[str] | None,
    output_format: str,
) -> dict:
    """Estimate the download size of one parquet key, from metadata only.

    Nothing here reads a data page. Two levels of pruning narrow the dataset to
    the request, both on metadata the download prunes on too:

    1. partitions (free, path-based): the `timestamp` buckets covering the date
       range, and the `polygon` partitions intersecting any bbox
    2. row groups (one footer read per surviving file): keep only those whose
       TIME/LATITUDE/LONGITUDE statistics overlap the request

    The surviving row groups give a ROW COUNT; the CSV width of one row comes
    from the schema (_csv_bytes_per_row). rows x width is the CSV text, then
    scaled to the ZIP.

    :param parquet_ds: the datasource
    :param bboxes: bboxes to prune with; empty means no spatial filter
    :param columns: requested columns; ignored
    :param output_format: the requested format
    :return: dict with uuid, key, format, estimated_uncompressed_bytes,
        estimated_output_bytes and notes
    """
    notes: list[str] = []
    if len(bboxes) > 1:
        notes.append(f"union of {len(bboxes)} polygon bboxes")
    if columns:
        # Aligned with the download: query_data in parquet_processor.py passes no
        # columns either, so the CSV always carries every column.
        log.info("column subsetting not implemented yet; ignoring columns %s", columns)
        notes.append(f"column subsetting not supported yet; columns skipped: {columns}")
    date_start = ensure_timezone(date_start)
    date_end = ensure_timezone(date_end)
    lat_name, lon_name, time_name = api.resolve_dim_names(uuid, key)

    # Cached on the datasource the API singleton holds - the first access does
    # the recursive S3 listing, later requests reuse it. Never re-create a
    # dataset per request: query_unique_value caches on id(dataset).
    dataset = parquet_ds.dataset

    log.debug(
        "_estimate_parquet_size: uuid=%s key=%s slice=[%s..%s] bboxes=%d format=%s",
        uuid,
        key,
        date_start,
        date_end,
        len(bboxes),
        output_format,
    )

    try:
        partition_expr = _partition_filter(dataset, date_start, date_end, bboxes, notes)
    except PolygonNotIntersectingError:
        # Genuinely no data for this area - not an error, a zero estimate.
        return _empty_estimate(
            uuid,
            key,
            output_format,
            note="no data partitions intersect the requested area",
        )

    # Keyed by path so a fragment can only be counted once, whatever matched it.
    fragments = list(
        {f.path: f for f in dataset.get_fragments(filter=partition_expr)}.values()
    )
    surviving = len(fragments)
    if surviving == 0:
        return _empty_estimate(
            uuid, key, output_format, note="no data files match the requested subset"
        )

    # One footer per fragment is one S3 GET, so cap the work and extrapolate.
    scale = 1.0
    if surviving > MAX_FRAGMENT_FOOTER_READS:
        step = ceil(surviving / MAX_FRAGMENT_FOOTER_READS)
        fragments = fragments[::step]
        scale = surviving / len(fragments)
        notes.append(f"sampled {len(fragments)} of {surviving} files, extrapolated")

    rows = 0
    for fragment in fragments:
        # .metadata reads the file FOOTER only. It is re-read per access, so it
        # is not retained beyond this iteration.
        metadata = fragment.metadata
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            if not _row_group_overlaps(
                row_group, time_name, lat_name, lon_name, date_start, date_end, bboxes
            ):
                continue
            rows += row_group.num_rows

    rows = int(rows * scale)

    # Row count -> CSV text -> the ZIP that is actually downloaded. The width
    # comes from the schema, NOT from the row groups' uncompressed bytes
    bytes_per_row = _csv_bytes_per_row(dataset)
    total_uncompressed = rows * bytes_per_row
    total_output = int(total_uncompressed * COMPRESSION_RATIO_CSV_GZIP)

    if bboxes:
        notes.append(
            "bbox upper bound: the download filters to the exact polygon(s), "
            "the estimate counts the bounding box"
        )
    notes.append(
        "row-group granularity: partially-matching row groups are counted "
        "whole (upper bound)"
    )
    notes.append(
        f"~{rows:,} rows across {surviving} file(s), ~{bytes_per_row:,} CSV "
        "bytes per row"
    )
    notes.append(
        f"estimated download size ~{_bytes_to_mb(total_output)} MB "
        f"(uncompressed ~{_bytes_to_mb(total_uncompressed)} MB)"
    )

    deduped_notes = list(dict.fromkeys(notes))

    log.debug(
        "_estimate_parquet_size: totals rows=%d bytes_per_row=%d uncompressed=%d "
        "output=%d",
        rows,
        bytes_per_row,
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


def _csv_bytes_per_row(dataset: pa_ds.Dataset) -> int:
    """Bytes one CSV row of `dataset` occupies, summed over every column.

    Uses the schema, not the files, so it costs no IO and still counts
    partition columns - their values live in the directory names, not in the
    parquet data, but they do end up in the CSV.

    Partition columns are measured from the paths; other columns use the
    per-type width, which assumes the widest value the type can print, so the
    result is an upper bound.
    """
    partition_widths = _partition_value_widths(dataset)
    return sum(
        partition_widths.get(field.name, _csv_value_bytes(field.type))
        + CSV_SEPARATOR_BYTES
        for field in dataset.schema
    )


def _partition_value_widths(dataset: pa_ds.Dataset) -> dict[str, int]:
    """{partition column: widest value length}, read off the fragment paths.

    Free - `query_unique_value` parses the paths of the already-listed
    fragments and caches per dataset. A column whose values cannot be read is
    simply absent, so the caller falls back to the per-type width."""
    partitioning = getattr(dataset, "partitioning", None)
    if partitioning is None:
        return {}

    widths = {}
    for name in partitioning.schema.names:
        try:
            values = query_unique_value(dataset, name)
        except Exception as e:
            log.warning("partition values for %s unreadable: %s", name, e)
            continue
        if values:
            widths[name] = max(len(str(value)) for value in values)
    return widths


def _csv_value_bytes(pa_type: pa.DataType) -> int:
    """Bytes one value of `pa_type` occupies once printed into a CSV cell.
    Anything not recognised is charged ASSUMED_STRING_BYTES."""
    if pa.types.is_floating(pa_type):
        return (
            CSV_BYTES_PER_FLOAT64 if pa_type.bit_width == 64 else CSV_BYTES_PER_FLOAT32
        )
    if pa.types.is_integer(pa_type):
        return CSV_BYTES_PER_INT.get(pa_type.bit_width, CSV_BYTES_PER_INT[64])
    if pa.types.is_boolean(pa_type):
        return CSV_BYTES_PER_BOOL
    if pa.types.is_timestamp(pa_type):
        return CSV_BYTES_PER_TIMESTAMP
    if pa.types.is_date(pa_type):
        return CSV_BYTES_PER_DATE
    if pa.types.is_null(pa_type):
        return CSV_BYTES_PER_NULL
    return ASSUMED_STRING_BYTES


def _partition_filter(
    dataset: pa_ds.Dataset,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    bboxes: list[BoundingBox],
    notes: list[str],
) -> Optional[pc.Expression]:
    """Build a PARTITION-ONLY expression for get_fragments.

    Only `timestamp` and `polygon` belong here - a row-level predicate would
    make get_fragments open the files; row-level pruning happens off the footers
    instead (_row_group_overlaps).

    :return: the expression, or None when neither dimension can be pruned
    :raises PolygonNotIntersectingError: no polygon partition intersects any bbox
    """
    parts = [
        _timestamp_partition_expr(dataset, date_start, date_end, notes),
        _polygon_partition_expr(dataset, bboxes, notes),
    ]
    parts = [p for p in parts if p is not None]
    if not parts:
        return None

    expression = parts[0]
    for part in parts[1:]:
        expression = expression & part
    return expression


def _timestamp_partition_expr(
    dataset: pa_ds.Dataset,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    notes: list[str],
) -> Optional[pc.Expression]:
    """Restrict to the `timestamp` partition buckets covering the date range,
    using the same boundary helper the download's time filter uses.

    Returns None (no time pruning, so an upper bound) when the dataset has no
    timestamp partitions or the boundaries cannot be worked out."""
    try:
        if not query_unique_value(dataset, "timestamp"):
            notes.append("no timestamp partitions; time pruning skipped (upper bound)")
            return None
        bucket_start, bucket_end = get_timestamps_boundary_values(
            dataset, date_start, date_end
        )
    except Exception as e:
        log.warning("timestamp partition pruning skipped: %s", e)
        notes.append("timestamp partitions unreadable; time pruning skipped")
        return None

    return (
        pc.field("timestamp") >= _timestamp_partition_scalar(dataset, bucket_start)
    ) & (pc.field("timestamp") <= _timestamp_partition_scalar(dataset, bucket_end))


def _timestamp_partition_scalar(dataset: pa_ds.Dataset, value) -> pa.Scalar:
    """Cast a partition boundary to the dataset's own `timestamp` field type.

    Hive partition types are inferred from directory names, so the same value is
    int32 in one dataset and a string in another; comparing an int64 literal
    against a string field raises ArrowNotImplementedError. (Mirrors
    DataQuery._timestamp_scalar, which is private to the library.)"""
    try:
        ts_type = dataset.schema.field("timestamp").type
    except KeyError:
        ts_type = pa.int64()

    if pa.types.is_string(ts_type) or pa.types.is_large_string(ts_type):
        return pa.scalar(str(int(value)), type=ts_type)
    return pa.scalar(int(value), type=ts_type)


def _polygon_partition_expr(
    dataset: pa_ds.Dataset, bboxes: list[BoundingBox], notes: list[str]
) -> Optional[pc.Expression]:
    """Restrict to the `polygon` partitions intersecting ANY requested bbox, so
    a partition shared by two bboxes is counted once.

    Returns None (no spatial pruning) when there is no spatial filter at all or
    the dataset is not partitioned by polygon.

    :raises PolygonNotIntersectingError: nothing intersects, i.e. no data
    """
    if not bboxes:
        # Empty bboxes means "no spatial filter" - same convention as the zarr
        # path. Do NOT fabricate a whole-globe box.
        return None

    try:
        partitions = query_unique_value(dataset, "polygon")
    except Exception as e:
        log.warning("polygon partition pruning skipped: %s", e)
        partitions = set()
    if not partitions:
        notes.append("no polygon partitions; spatial pruning skipped (upper bound)")
        return None

    requested = [
        box(bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat) for bbox in bboxes
    ]
    matching = [
        hex_wkb
        for hex_wkb in partitions
        if _partition_intersects_any(hex_wkb, requested)
    ]
    if not matching:
        raise PolygonNotIntersectingError(
            "No polygon partition intersects the requested area"
        )

    return pc.field("polygon").isin(matching)


def _partition_intersects_any(hex_wkb: str, requested: list) -> bool:
    """Whether one `polygon` partition value (WKB hex) intersects any requested
    shape. An unreadable value counts as intersecting, so a bad directory name
    can only over-estimate, never drop real data."""
    try:
        partition_shape = wkb.loads(bytes.fromhex(hex_wkb))
    except Exception as e:
        log.warning("unreadable polygon partition %s: %s", hex_wkb, e)
        return True
    return any(partition_shape.intersects(shape) for shape in requested)


def _row_group_overlaps(
    row_group,
    time_name: str | None,
    lat_name: str | None,
    lon_name: str | None,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    bboxes: list[BoundingBox],
) -> bool:
    """Whether one row group can hold rows the download would keep, judged only
    on the min/max statistics in the footer.

    Missing statistics mean "cannot rule it out", so the row group is kept and
    the estimate stays an upper bound."""
    ranges = _row_group_column_ranges(row_group)

    time_range = ranges.get(time_name) if time_name else None
    if time_range is not None:
        low, high = _as_utc_timestamp(time_range[0]), _as_utc_timestamp(time_range[1])
        if low is not None and high is not None:
            if high < date_start or low > date_end:
                return False

    if bboxes:
        lat_range = ranges.get(lat_name) if lat_name else None
        lon_range = ranges.get(lon_name) if lon_name else None
        if lat_range is not None and lon_range is not None:
            # The row group's own bounding box; keep it if it overlaps any
            # requested box in BOTH axes.
            if not any(
                bbox.min_lat <= lat_range[1]
                and lat_range[0] <= bbox.max_lat
                and bbox.min_lon <= lon_range[1]
                and lon_range[0] <= bbox.max_lon
                for bbox in bboxes
            ):
                return False

    return True


def _row_group_column_ranges(row_group) -> dict:
    """{column name: (min, max)} for the columns of one row group that carry
    statistics. Columns without them are simply absent."""
    ranges = {}
    for i in range(row_group.num_columns):
        column = row_group.column(i)
        statistics = column.statistics
        if statistics is not None and statistics.has_min_max:
            ranges[column.path_in_schema] = (statistics.min, statistics.max)
    return ranges


def _as_utc_timestamp(value) -> Optional[pd.Timestamp]:
    """A time statistic as a UTC-aware Timestamp, or None when it is not a date.

    pyarrow returns a NAIVE Timestamp for a timestamp column, which cannot be
    compared with the request's UTC bounds - ensure_timezone assumes UTC. A time
    column stored as a plain number (epoch seconds) is ambiguous, so we return
    None and let the caller keep the row group rather than guess the unit."""
    if not isinstance(value, (pd.Timestamp, datetime, np.datetime64)):
        return None
    return ensure_timezone(pd.Timestamp(value))


def _empty_estimate(
    uuid: str,
    key: str,
    output_format: str,
    note: str = "requested date range is outside the dataset's temporal extent",
) -> dict:
    """Zero-size estimate, returned when the request cannot match any data.
    `note` says why."""
    return {
        "uuid": uuid,
        "key": key,
        "format": output_format,
        "estimated_uncompressed_bytes": 0,
        "estimated_output_bytes": 0,
        "notes": note,
    }
