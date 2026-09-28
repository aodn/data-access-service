"""PyArrow filters and a bounded scan shared by the parquet reads.

`API.get_dataset` (string time columns) and `API.iter_parquet_batches` (the
subset job) both need the same time, bbox, and equality predicate. Building
that here keeps the literal casting in one place: a hive column such as
`polygon` is not always on the file schema, and a string time column cannot
be compared with a timestamp.
"""

from typing import Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import compute as pc

from aodn_cloud_optimised.lib.DataQuery import create_bbox_filter, create_time_filter

from data_access_service.utils.time_column_utils import TimeColumn, build_time_filter

# One decoded batch, and no extra batches queued behind it. The library
# get_data path uses to_table(use_threads=True), whose default readahead holds
# many row groups at once; a month of argo in that path sits near the 8 GB
# task limit before any row is written.
PARQUET_BATCH_ROWS = 4096


def and_filters(*filters: pc.Expression | None) -> pc.Expression | None:
    """AND the expressions that are present. None when every argument is None."""
    combined = None
    for item in filters:
        if item is None:
            continue
        combined = item if combined is None else combined & item
    return combined


def scalar_equality_filter(
    dataset, scalar_filter: Optional[dict]
) -> pc.Expression | None:
    """AND of equality terms, including hive partition columns.

    Same literal casting as ParquetDataSource.get_data: a partition column such
    as `polygon` is not always on the file schema, so the partition schema is
    consulted before falling back to the value's own type.
    """
    if not scalar_filter:
        return None

    full_schema = dataset.schema
    partitioning = getattr(dataset, "partitioning", None)
    part_schema = getattr(partitioning, "schema", None)
    if part_schema is not None:
        for field in part_schema:
            if field.name not in full_schema.names:
                full_schema = full_schema.append(field)

    expr = None
    for key, value in scalar_filter.items():
        if key in full_schema.names:
            col_type = full_schema.field(key).type
        else:
            col_type = pa.scalar(value).type
        term = pc.field(key) == pa.scalar(value, type=col_type)
        expr = term if expr is None else expr & term
    return expr


def bbox_filter(dataset, lat_min, lat_max, lon_min, lon_max, lat_varname, lon_varname):
    """Library bbox predicate, or None when any bound is missing."""
    if None in (lat_min, lat_max, lon_min, lon_max):
        return None
    bbox_kwargs = {}
    if lat_varname is not None:
        bbox_kwargs["lat_varname"] = lat_varname
    if lon_varname is not None:
        bbox_kwargs["lon_varname"] = lon_varname
    return create_bbox_filter(
        dataset,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        **bbox_kwargs,
    )


def typed_time_filter(
    dataset,
    time_column: TimeColumn,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    lat_varname,
    lon_varname,
    scalar_filter,
) -> pc.Expression:
    """Time, bbox, and equality filter. The time literal matches the stored type."""
    return and_filters(
        build_time_filter(dataset, time_column, date_start, date_end),
        bbox_filter(
            dataset, lat_min, lat_max, lon_min, lon_max, lat_varname, lon_varname
        ),
        scalar_equality_filter(dataset, scalar_filter),
    )


def parquet_data_filter(
    dataset,
    *,
    time_column: TimeColumn | None,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    query_start: str | None,
    query_end: str | None,
    time_varname: str | None,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    lat_varname,
    lon_varname,
    scalar_filter,
) -> pc.Expression | None:
    """Predicate for one parquet window.

    A non-timestamp time column uses :func:`typed_time_filter`. A timestamp
    column uses the library ``create_time_filter``, which also checks the
    dataset extent. ``query_start is None`` means there is no temporal extent,
    so the time predicate is left out.
    """
    if time_column is not None and not time_column.is_timestamp:
        return typed_time_filter(
            dataset,
            time_column,
            date_start,
            date_end,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            lat_varname,
            lon_varname,
            scalar_filter,
        )

    time_expr = None
    if query_start is not None:
        time_kwargs = {}
        if time_varname is not None:
            time_kwargs["time_varname"] = time_varname
        time_expr = create_time_filter(
            dataset,
            date_start=query_start,
            date_end=query_end,
            **time_kwargs,
        )
    return and_filters(
        bbox_filter(
            dataset, lat_min, lat_max, lon_min, lon_max, lat_varname, lon_varname
        ),
        time_expr,
        scalar_equality_filter(dataset, scalar_filter),
    )


def scan_parquet_batches(
    dataset: ds.Dataset,
    data_filter: pc.Expression | None,
    columns: Optional[list[str]],
    batch_size: int = PARQUET_BATCH_ROWS,
) -> Iterator[pa.RecordBatch]:
    """Yield `dataset` one batch at a time.

    ``use_threads=False`` and a readahead of one keep a single batch decoded.
    The default scanner prefetches 16 batches across 4 fragments.
    """
    scanner = dataset.scanner(
        columns=columns,
        filter=data_filter,
        batch_size=batch_size,
        batch_readahead=1,
        fragment_readahead=1,
        use_threads=False,
    )
    return scanner.to_batches()
