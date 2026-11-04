"""Find the data-index files of a collection in the data-index S3 Tables.

The collection-mapping table has one row per file version, with its collection
uuid, time range and bounding box. It also keeps old versions and deleted
files, so we join the live file inventory to keep the current files only.
We pick whole files that overlap the request; the files are not cut.

A collection has one key for its data-index files: ORIGINAL_NETCDF_KEY.
The table's physical_dataset_id is not used: the data team says it is a
system placeholder (always __DEFAULT__), not meant for users.
"""

import logging
from typing import Any, List, Optional

import pandas as pd

from data_access_service.core.duckdbclients.dataindexclients import (
    DataIndexDuckDBClient,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.data_index_types import DataIndexFile

logger = logging.getLogger(__name__)

# Value of the table's ``format`` column for a NetCDF file.
NETCDF_FORMAT = "nc"

# The one key of a collection's data-index files, like the dataset
# name of a cloud-optimised collection.
ORIGINAL_NETCDF_KEY = "original_netcdfs"


def _time_sql(column: str) -> str:
    """Parse the time text. It comes in several formats, so try two parsers.

    A value neither can read (e.g. a plain number of days) becomes NULL.
    """
    return (
        f"coalesce(TRY_CAST({column} AS TIMESTAMPTZ), "
        f"TRY_STRPTIME({column}, '%Y%m%dT%H%M%SZ'))"
    )


def _files_sql(table: str, live_table: Optional[str] = None) -> str:
    """CTE ``f``: the NetCDF rows of one uuid, with parsed times.

    With ``live_table``, an inner join drops old versions and deleted files.
    Binds the uuid and the format. Column order matches DataIndexFile.
    """
    join = (
        f"JOIN {live_table} s ON s.bucket = m.bucket AND s.key = m.key"
        " AND s.version_id = m.version_id"
        if live_table
        else ""
    )
    return f"""
        WITH f AS (
            SELECT m.bucket, m.key,
                   {_time_sql("m.time_coverage_start")} AS t_start,
                   {_time_sql("m.time_coverage_end")} AS t_end,
                   m.geospatial_lat_min AS lat_min,
                   m.geospatial_lat_max AS lat_max,
                   m.geospatial_lon_min AS lon_min,
                   m.geospatial_lon_max AS lon_max
            FROM {table} m
            {join}
            WHERE m.metadata_uuid = ? AND m.format = ?
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


def list_data_index_uuids(client: DataIndexDuckDBClient) -> frozenset[str]:
    """The data-index uuids: the uuids that have NetCDF files in the data index.

    For the metadata endpoint, so no join with the live inventory.
    """
    rows = client.execute(
        f"SELECT DISTINCT metadata_uuid FROM {client.qualified_table}"
        " WHERE format = ? AND metadata_uuid IS NOT NULL",
        [NETCDF_FORMAT],
    ).fetchall()
    return frozenset(row[0] for row in rows)


def find_data_index_files(
    client: DataIndexDuckDBClient,
    uuid: str,
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
    bboxes: Optional[List[BoundingBox]] = None,
) -> List[DataIndexFile]:
    """The current files of ``uuid`` that overlap ``start..end`` and any of ``bboxes``.

    A file with no time (when a time is given) or no bbox (when an area is
    given) cannot be matched, so it is left out. The count of those rows is
    logged. With no filter, every file is listed, even one with no time or bbox.

    :param start: the request start; None means no lower time bound
    :param end: the request end; None means no upper time bound
    :param bboxes: the request area; empty or None means no area filter
    """
    params: List[Any] = [uuid, NETCDF_FORMAT]
    # TRUE keeps the WHERE valid when there is no filter.
    where = ["TRUE"]
    missing: List[str] = []
    if start is not None:
        where.append("t_end >= ?")
        params.append(start)
        missing.append("t_end IS NULL")
    if end is not None:
        where.append("t_start <= ?")
        params.append(end)
        missing.append("t_start IS NULL")
    if bboxes:
        where.append(_bbox_sql(bboxes, params))
        missing += [
            f"{column} IS NULL"
            for column in ("lat_min", "lat_max", "lon_min", "lon_max")
        ]

    files_sql = _files_sql(client.qualified_table, client.qualified_s3_metadata_table)
    rows = client.execute(
        f"{files_sql} SELECT * FROM f WHERE {' AND '.join(where)} ORDER BY key",
        params,
    ).fetchall()

    # Only for the log, so no join: it scans the whole live inventory again.
    # Old versions are counted too. Nothing is left out with no filter.
    if missing:
        skipped = client.execute(
            f"{_files_sql(client.qualified_table)} SELECT count(*) FROM f"
            f" WHERE {' OR '.join(missing)}",
            [uuid, NETCDF_FORMAT],
        ).fetchone()[0]
        if skipped:
            logger.warning(
                "uuid=%s: %d rows skipped, no time or bbox in the data index",
                uuid,
                skipped,
            )

    return [DataIndexFile(*row) for row in rows]
