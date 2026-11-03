"""Find the original NetCDF files of a collection in the data-index S3 Table.

The table has one row per live file, with its collection uuid, time range and
bounding box. We pick whole files that overlap the request; the files are not
cut.
"""

import logging
from typing import Any, List, Optional

import pandas as pd

from data_access_service.core.duckdbclients.dataindexclients import (
    DataIndexDuckDBClient,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.data_index_types import OriginalNetcdf

logger = logging.getLogger(__name__)

# Value of the table's ``format`` column for a NetCDF file.
NETCDF_FORMAT = "nc"


def _time_sql(column: str) -> str:
    """Parse the time text. It comes in several formats, so try two parsers.

    A value neither can read (e.g. a plain number of days) becomes NULL.
    """
    return (
        f"coalesce(TRY_CAST({column} AS TIMESTAMPTZ), "
        f"TRY_STRPTIME({column}, '%Y%m%dT%H%M%SZ'))"
    )


def _files_sql(table: str) -> str:
    """CTE ``f``: the NetCDF rows of one uuid, with parsed times.

    Binds the uuid and the format. Column order matches OriginalNetcdf.
    """
    return f"""
        WITH f AS (
            SELECT bucket, key,
                   {_time_sql("time_coverage_start")} AS t_start,
                   {_time_sql("time_coverage_end")} AS t_end,
                   geospatial_lat_min AS lat_min,
                   geospatial_lat_max AS lat_max,
                   geospatial_lon_min AS lon_min,
                   geospatial_lon_max AS lon_max
            FROM {table}
            WHERE metadata_uuid = ? AND format = ?
        )
    """


def _bbox_sql(bboxes: List[BoundingBox], params: List[Any]) -> str:
    """True when the file's bbox overlaps at least one request bbox.

    Some files store lon in 0-360, so each request lon is also tried + 360.
    Appends the bound values to ``params``.
    """
    groups = []
    for bbox in bboxes:
        groups.append(
            "(lat_min <= ? AND lat_max >= ?"
            " AND ((lon_min <= ? AND lon_max >= ?)"
            " OR (lon_min <= ? + 360 AND lon_max >= ? + 360)))"
        )
        params += [
            bbox.max_lat,
            bbox.min_lat,
            bbox.max_lon,
            bbox.min_lon,
            bbox.max_lon,
            bbox.min_lon,
        ]
    return "(" + " OR ".join(groups) + ")"


def find_original_netcdfs(
    client: DataIndexDuckDBClient,
    uuid: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    bboxes: Optional[List[BoundingBox]] = None,
) -> List[OriginalNetcdf]:
    """The files of ``uuid`` that overlap ``start..end`` and any of ``bboxes``.

    A file with no time (or no bbox, when an area is given) cannot be matched,
    so it is left out. The count of those files is logged.

    :param bboxes: the request area; empty or None means no area filter
    """
    files_sql = _files_sql(client.qualified_table)
    params: List[Any] = [uuid, NETCDF_FORMAT, end, start]
    where = ["t_start <= ?", "t_end >= ?"]
    missing = ["t_start IS NULL", "t_end IS NULL"]
    if bboxes:
        where.append(_bbox_sql(bboxes, params))
        missing += [
            f"{column} IS NULL"
            for column in ("lat_min", "lat_max", "lon_min", "lon_max")
        ]

    rows = client.execute(
        f"{files_sql} SELECT * FROM f WHERE {' AND '.join(where)} ORDER BY key",
        params,
    ).fetchall()

    skipped = client.execute(
        f"{files_sql} SELECT count(*) FROM f WHERE {' OR '.join(missing)}",
        [uuid, NETCDF_FORMAT],
    ).fetchone()[0]
    if skipped:
        logger.warning(
            "uuid=%s: %d files skipped, no time or bbox in the data index",
            uuid,
            skipped,
        )

    return [OriginalNetcdf(*row) for row in rows]
