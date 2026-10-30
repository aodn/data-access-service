"""Query the live collection-mapping table in the data-index S3 Tables bucket.

The table (``data_index.collection_mapping.live``) is one row per physical
dataset file, carrying its spatial box (``geospatial_lat_min/max``,
``geospatial_lon_min/max``) and its temporal cover (``time_coverage_start`` /
``time_coverage_end``, ISO-8601 UTC strings). The ``/data-index`` routes let a
caller find the files whose box intersects a drawn polygon and whose cover
overlaps a time window.

This module owns one shared :class:`DataIndexDuckDBClient`, built on first use
(or eagerly by the server lifespan via :func:`init_client`). The DuckDB
``spatial`` extension is loaded once so ``ST_Intersects`` can do a true
polygon/box test rather than a bounding-box-only comparison.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional

import pandas as pd
from shapely.geometry.base import BaseGeometry

from data_access_service.core.duckdbclients.dataindexclients import (
    DataIndexDuckDBClient,
)

log = logging.getLogger(__name__)

# The columns returned to the caller, in a stable order.
RESULT_COLUMNS = (
    "metadata_uuid",
    "bucket",
    "key",
    "version_id",
    "format",
    "physical_dataset_id",
    "time_coverage_start",
    "time_coverage_end",
    "geospatial_lon_min",
    "geospatial_lat_min",
    "geospatial_lon_max",
    "geospatial_lat_max",
)

_client: Optional[DataIndexDuckDBClient] = None
_client_lock = threading.Lock()
_spatial_loaded = False


def _get_client() -> DataIndexDuckDBClient:
    """The shared client, built (and ``spatial``-loaded) on first use."""
    global _client, _spatial_loaded
    with _client_lock:
        if _client is None:
            _client = DataIndexDuckDBClient()
        if not _spatial_loaded:
            # Needed for ST_Intersects / ST_MakeEnvelope below. Loaded once;
            # the extension is cheap but LOAD on every query is wasteful.
            _client.execute("INSTALL spatial; LOAD spatial;")
            _spatial_loaded = True
        return _client


def init_client() -> None:
    """Build the client now so a broken read path fails at startup, not on the
    first request. The server lifespan calls this."""
    _get_client()


def close_client() -> None:
    """Close the shared client (called from the lifespan shutdown)."""
    global _client, _spatial_loaded
    with _client_lock:
        client = _client
        _client = None
        _spatial_loaded = False
    if client is not None:
        client.close()


def query_collection_mapping(
    *,
    geometry: BaseGeometry,
    start_time: Optional[pd.Timestamp],
    end_time: Optional[pd.Timestamp],
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Rows whose spatial box intersects ``geometry`` and whose temporal cover
    overlaps ``[start_time, end_time]``.

    ``geometry`` is a shapely geometry in WGS84 lon/lat (the merged polygon the
    portal drew). ``start_time`` / ``end_time`` are UTC-aware timestamps; either
    may be ``None`` to leave that side of the window open. Overlap (not
    containment) is used for time so a file that merely starts before and ends
    after the window still matches.

    All user values are bound as parameters; nothing is interpolated into SQL.
    """
    client = _get_client()
    table = client.qualified_table

    clauses = [
        # Rows with no box cannot be tested for intersection; drop them so the
        # spatial predicate does not see NULL envelopes.
        "geospatial_lon_min IS NOT NULL",
        "geospatial_lat_min IS NOT NULL",
        "geospatial_lon_max IS NOT NULL",
        "geospatial_lat_max IS NOT NULL",
        (
            "ST_Intersects("
            "ST_MakeEnvelope("
            "geospatial_lon_min, geospatial_lat_min, "
            "geospatial_lon_max, geospatial_lat_max), "
            "ST_GeomFromText(?))"
        ),
    ]
    params: list[Any] = [geometry.wkt]

    # time_coverage_* are ISO-8601 strings; cast both sides to TIMESTAMPTZ so
    # the comparison is on the instant, not on string bytes. try_cast keeps a
    # malformed stored value from failing the whole query (it drops that row).
    if end_time is not None:
        clauses.append("try_cast(time_coverage_start AS TIMESTAMPTZ) <= ?")
        params.append(end_time.isoformat())
    if start_time is not None:
        clauses.append("try_cast(time_coverage_end AS TIMESTAMPTZ) >= ?")
        params.append(start_time.isoformat())

    columns = ", ".join(RESULT_COLUMNS)
    sql = f"SELECT {columns} FROM {table} WHERE " + " AND ".join(clauses)
    sql += " ORDER BY time_coverage_start"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    rel = client.execute(sql, params)
    result_cols = [d[0] for d in rel.description]
    return [dict(zip(result_cols, row)) for row in rel.fetchall()]
