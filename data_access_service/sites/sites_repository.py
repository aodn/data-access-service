"""A base Parquet repository over a DuckDB client.

``ParquetRepository`` binds one :class:`ParquetDuckDBClient` to one Parquet
dataset — each dataset gets its own subclass (see :mod:`mooring`) that declares
where it lives and adds dataset-specific reads.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from typing import ClassVar
import re

import pandas as pd

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import ParquetDuckDBClient


def quote_ident(name: str) -> str:
    """Quote an SQL identifier (table/column) so it can't break out of context.

    Identifiers are interpolated as text (they can't be bound as ``?``
    parameters), so they go through here. Quoting also makes the identifier
    case-sensitive, so the name must match the stored column's case.
    """
    return '"' + str(name).replace('"', '""') + '"'


class ParquetRepository(ABC):
    """Abstract base for one timeseries Parquet dataset: where it lives plus its reads.

    Abstract: this class can't be instantiated directly — use a dataset subclass
    (see :mod:`mooring`). Subclasses set the location/schema attributes below;
    any subclass that omits a required one fails at import (see
    :meth:`__init_subclass__`). Because a repository is bound to a single table,
    none of the read methods take a table name.

    Location (set per dataset):
      ``dataset``  — S3/local prefix, the directory above the Hive partitions.
      ``table``    — name of the in-memory table the dataset is loaded into.

    Schema (default to the common IMOS timeseries names; override as needed):
      ``time_column``, ``site_column``, ``latitude_column``, ``longitude_column``.
    """

    table: ClassVar[str]
    dataset: ClassVar[str]
    bucket: ClassVar[str]
    backup_bucket: ClassVar[str]
    backup_dataset: ClassVar[str]
    time_column: ClassVar[str]
    site_column: ClassVar[str]
    latitude_column: ClassVar[str]
    longitude_column: ClassVar[str]
    value_columns: ClassVar[Sequence[str]]
    group_column: ClassVar[str | None] = None
    value_columns_quality_control_columns: ClassVar[Sequence[str]] = ()
    partition_column: ClassVar[str | None] = "timestamp"

    def __init_subclass__(cls, **kwargs) -> None:
        """Require every subclass to define the no-default class attributes.

        The required set is derived automatically: each name annotated on this
        base without a default value (so ``group_column``, which defaults to
        ``None``, is exempt). A subclass missing any of them raises ``TypeError``
        at import — naming exactly what's absent — rather than failing lazily on
        first access.
        """
        super().__init_subclass__(**kwargs)
        required = [
            n
            for n in ParquetRepository.__annotations__
            if n not in ParquetRepository.__dict__
        ]
        missing = [n for n in required if not hasattr(cls, n)]
        if missing:
            raise TypeError(
                f"{cls.__name__} must define class attributes: {', '.join(missing)}"
            )
        qc_columns = cls.value_columns_quality_control_columns
        if qc_columns and len(qc_columns) != len(cls.value_columns):
            raise TypeError(
                f"{cls.__name__}.value_columns_quality_control_columns must be "
                "empty or the same length as value_columns"
            )

    def __init__(self, session: ParquetDuckDBClient) -> None:
        if type(self) is ParquetRepository:
            raise TypeError(
                "ParquetRepository is abstract; instantiate a dataset subclass"
            )
        self.session = session
        self._configure_s3()
        self._configure_backup_s3()

    @property
    def incremental_lookback_days(self) -> int:
        """How far back :meth:`load_incremental` re-reads by default.

        A shared value from ``parquet.config.incremental_lookback_days``
        (via the bound session), not per-dataset -- every repository is
        expected to use the same lookback window.
        """
        return self.session.incremental_lookback_days

    @property
    def value_quality_control_map(self) -> dict[str, str]:
        """Map each ``value_columns`` entry to its QC column, for those that have one.

        Built by pairing ``value_columns`` and ``value_columns_quality_control_columns``
        index-for-index. Empty when the dataset declares no QC columns.
        """
        if not self.value_columns_quality_control_columns:
            return {}
        return dict(zip(self.value_columns, self.value_columns_quality_control_columns))

    @property
    def load_columns(self) -> list[str]:
        """The column subset to materialize on :meth:`load`.

        Derived from the schema columns (time, site, location, plus
        ``group_column`` when set), the dataset's ``value_columns``, and any
        ``value_columns_quality_control_columns`` — the union of everything
        any read needs, with no duplicates.
        """
        cols = [
            self.time_column,
            self.site_column,
            self.latitude_column,
            self.longitude_column,
        ]
        if self.group_column is not None:
            cols.append(self.group_column)
        cols += [c for c in self.value_columns if c not in cols]
        cols += [c for c in self.value_columns_quality_control_columns if c not in cols]
        return cols

    def _configure_s3(self) -> None:
        """Create the S3 secret DuckDB uses to read the primary dataset."""
        self.session.create_s3_secret(self.bucket)

    def _configure_backup_s3(self) -> None:
        """Create the S3 secret DuckDB uses to read the backup dataset."""
        self.session.create_s3_secret(self.backup_bucket)

    def load(self) -> ParquetRepository:
        """Full load: read the entire ``dataset`` and replace ``table``.

        Temporarily raises the shared connection's thread count to
        ``full_load_threads`` for this heavier, infrequent read, then
        restores it. Atomic via ``CREATE OR REPLACE TABLE``: a failed read
        rolls back and leaves the existing table intact.
        """
        cols = ", ".join(quote_ident(c) for c in self.load_columns)
        self.session.set_threads(self.session.full_load_threads)
        try:
            self.session.execute(
                f"""
                CREATE OR REPLACE TABLE {quote_ident(self.table)} AS
                SELECT {cols}
                FROM read_parquet(
                    '{self.dataset}/**/*.parquet',
                    hive_partitioning=true,
                    union_by_name=true
                )"""
            )
        finally:
            self.session.set_threads(self.session.threads)
        return self

    def _incremental_cutoff(self, lookback_days: int) -> pd.Timestamp:
        """Point in time ``lookback_days`` before now.

        "Now" is UTC: this codebase treats naive timestamps as UTC throughout
        (see the ``TIME`` column), and the container's own clock is pinned to
        UTC (``ENV TZ=UTC`` in the Dockerfile), but the tz-naive local
        conversion is made explicit here so this does not silently depend on
        that.
        """
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        return now - pd.Timedelta(days=lookback_days)

    def _qualifying_partition_globs(self, cutoff_epoch: int) -> list[str]:
        """Glob patterns for the partition containing ``cutoff_epoch`` and any after it.

        Lists the dataset's files to find real ``partition_column`` values —
        the latest one at-or-before the cutoff must contain it, since
        partitions are contiguous. Each pattern is the matched path's prefix
        up to that partition segment plus one trailing ``**/*.parquet``
        (DuckDB rejects more than one ``**`` per path). Empty if no files
        exist yet.
        """
        pattern = re.compile(rf"{re.escape(self.partition_column)}=(-?\d+)")
        rows = self.session.execute(
            "SELECT file FROM glob(?)", [f"{self.dataset}/**/*.parquet"]
        ).fetchall()
        parsed = [
            (int(m.group(1)), path[: m.end()])
            for (path,) in rows
            if (m := pattern.search(path))
        ]
        if not parsed:
            return []
        values = {v for v, _ in parsed}
        eligible_before = [v for v in values if v <= cutoff_epoch]
        threshold = max(eligible_before) if eligible_before else min(values)
        prefixes = sorted({prefix for v, prefix in parsed if v >= threshold})
        return [f"{prefix}/**/*.parquet" for prefix in prefixes]

    def load_incremental(self, lookback_days: int | None = None) -> ParquetRepository:
        """Incremental load: read only the files from
        :meth:`_qualifying_partition_globs` (the last ``lookback_days``),
        never the whole ``dataset``.
        """
        n = self.incremental_lookback_days if lookback_days is None else lookback_days
        cutoff = self._incremental_cutoff(n)
        cutoff_iso = cutoff.isoformat()

        cols = ", ".join(quote_ident(c) for c in self.load_columns)
        time = quote_ident(self.time_column)

        source: object = f"{self.dataset}/**/*.parquet"
        if self.partition_column is not None:
            cutoff_epoch = int(cutoff.tz_localize("UTC").timestamp())
            globs = self._qualifying_partition_globs(cutoff_epoch)
            if globs:
                source = globs

        self.session.execute(
            f"DELETE FROM {quote_ident(self.table)} WHERE {time} >= ?",
            [cutoff_iso],
        )
        self.session.execute(
            f"""
            INSERT INTO {quote_ident(self.table)}
            SELECT {cols}
            FROM read_parquet(
                ?,
                hive_partitioning=true,
                union_by_name=true
            )
            WHERE {time} >= ?""",
            [source, cutoff_iso],
        )
        return self

    def load_backup(self) -> ParquetRepository:
        """Seed the table from the BACKUP snapshot (a single flat Parquet file).

        ``backup_dataset`` is the last-known-good snapshot this app writes via
        :meth:`write_backup` — one flat file, not a partitioned directory, so it
        is read directly. On a first-ever run no backup exists yet and this
        raises; the caller treats that as "nothing to seed".
        """
        self.session.execute(
            f"""
            CREATE OR REPLACE TABLE {quote_ident(self.table)} AS
            SELECT * FROM read_parquet('{self.backup_dataset}')
            """
        )
        return self

    def write_backup(self) -> None:
        """Snapshot the current table to ``backup_dataset`` as one Parquet file.

        Called after a successful primary :meth:`load` so the backup always
        mirrors the latest good data. Best-effort for the caller: a failure here
        does not affect the already-loaded table.
        """
        self.session.execute(
            f"""
            COPY (SELECT * FROM {quote_ident(self.table)})
            TO '{self.backup_dataset}' (FORMAT PARQUET)
            """
        )

    def is_loaded(self) -> bool:
        """Whether this dataset's table exists yet (a load has committed).

        On an on-disk database the table survives restarts, so this is true
        immediately on startup once any prior run loaded it — it only returns
        false on the very first run, before the first background load commits.
        """
        row = self.session.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [self.table],
        ).fetchone()
        return row is not None

    def sites_in_date_range(self, start: str | None = None, end: str | None = None):
        """One row per site with the lat/lon and TIME of its latest record.

        Uses ``arg_max`` to pick the LAT/LON belonging to each site's most
        recent record. ``start`` / ``end`` (inclusive ISO 8601 strings) bound
        the records considered, and either may be given alone:

          * neither       — all sites.
          * ``start`` only — records at or after ``start`` (i.e. up to now).
          * ``end`` only   — records at or before ``end``.
          * both          — records within ``[start, end]``.
        """
        site = quote_ident(self.site_column)
        time = quote_ident(self.time_column)
        lat = quote_ident(self.latitude_column)
        lon = quote_ident(self.longitude_column)

        sql = (
            f"SELECT {site}, "
            f"arg_max({lat}, {time}) AS latitude, "
            f"arg_max({lon}, {time}) AS longitude, "
            f"max({time}) AS time "
            f"FROM {quote_ident(self.table)} "
        )
        conditions = []
        params = []
        if start is not None:
            conditions.append(f"{time} >= ?")
            params.append(start)
        if end is not None:
            conditions.append(f"{time} <= ?")
            params.append(end)
        if conditions:
            sql += "WHERE " + " AND ".join(conditions) + " "
        sql += f"GROUP BY {site} ORDER BY {site}"

        return self.session.execute(sql, params or None).df()

    def latest_time(self):
        """Return the single most recent TIME across all rows."""
        (value,) = self.session.execute(
            f"SELECT max({quote_ident(self.time_column)}) "
            f"FROM {quote_ident(self.table)}"
        ).fetchone()
        return value

    def site_details(self, site: str, start: str | None = None, end: str | None = None):
        """Return observation rows for one site, optionally within a time range.

        Selects this dataset's ``value_columns`` alongside the required columns
        (time, location, plus ``group_column`` when this dataset groups by one).
        ``start`` / ``end`` (inclusive ISO 8601 strings) bound the rows and
        either may be given alone:

          * neither       — every record for the site.
          * ``start`` only — records at or after ``start`` (i.e. up to now).
          * ``end`` only   — records at or before ``end``.
          * both          — records within ``[start, end]``.

        Rows are ordered by ``group_column`` then time for a grouped dataset
        (mooring, by sensor depth) and by time alone otherwise (wave buoy).

        Any ``value_columns`` entry paired with a QC column (via
        ``value_columns_quality_control_columns``) is nulled out row-by-row
        wherever its QC value isn't ``1`` — the rest of that row, including
        other value columns, is left untouched.
        """
        required = [self.time_column, self.latitude_column, self.longitude_column]
        if self.group_column is not None:
            required.append(self.group_column)
        qc_map = self.value_quality_control_map
        value_exprs = []
        for c in self.value_columns:
            if c in required:
                continue
            if c in qc_map:
                value_exprs.append(
                    f"CASE WHEN {quote_ident(qc_map[c])} = 1 THEN {quote_ident(c)} "
                    f"ELSE NULL END AS {quote_ident(c)}"
                )
            else:
                value_exprs.append(quote_ident(c))
        cols = ", ".join([quote_ident(c) for c in required] + value_exprs)

        time = quote_ident(self.time_column)
        order = time
        if self.group_column is not None:
            order = f"{quote_ident(self.group_column)}, {time}"

        conditions = [f"{quote_ident(self.site_column)} = ?"]
        params = [site]
        if start is not None:
            conditions.append(f"{time} >= ?")
            params.append(start)
        if end is not None:
            conditions.append(f"{time} <= ?")
            params.append(end)
        where = " AND ".join(conditions)

        return self.session.execute(
            f"SELECT {cols} FROM {quote_ident(self.table)} "
            f"WHERE {where} "
            f"ORDER BY {order}",
            params,
        ).df()


class MooringRepository(ParquetRepository):
    """Reads over the mooring-timeseries realtime-QC dataset."""

    config: Config = Config.get_config()

    table: ClassVar[str] = "mooring_timeseries_realtime_qc"
    bucket: ClassVar[str] = config.get_parquets_config().co_bucket
    backup_bucket: ClassVar[str] = config.get_mooring_backup_bucket_name()
    dataset: ClassVar[str] = f"s3://{bucket}/{table}.parquet"
    backup_dataset: ClassVar[str] = (
        f"s3://{backup_bucket}/imoslive/MOORING/{table}.parquet"
    )
    value_columns: ClassVar[tuple[str, ...]] = ("TEMP", "PSAL", "DOX1")
    value_columns_quality_control_columns: ClassVar[tuple[str, ...]] = (
        "TEMP_quality_control",
        "PSAL_quality_control",
        "DOX1_quality_control",
    )
    group_column: ClassVar[str] = "NOMINAL_DEPTH"
    time_column: ClassVar[str] = "TIME"
    site_column: ClassVar[str] = "site_code"
    latitude_column: ClassVar[str] = "LATITUDE"
    longitude_column: ClassVar[str] = "LONGITUDE"


class WaveBuoyRepository(ParquetRepository):
    """Reads over the realtime (non-QC) wave-buoy dataset."""

    config: Config = Config.get_config()

    table: ClassVar[str] = "wave_buoy_realtime_nonqc"
    bucket: ClassVar[str] = config.get_parquets_config().co_bucket
    backup_bucket: ClassVar[str] = config.get_wave_buoy_backup_bucket_name()
    dataset: ClassVar[str] = f"s3://{bucket}/{table}.parquet"
    backup_dataset: ClassVar[str] = (
        f"s3://{backup_bucket}/imoslive/BUOY/{table}.parquet"
    )
    value_columns: ClassVar[tuple[str, ...]] = ("WSSH", "SSWMD", "WPFM", "WPMH", "WHTH")
    time_column: ClassVar[str] = "TIME"
    site_column: ClassVar[str] = "site_name"
    latitude_column: ClassVar[str] = "LATITUDE"
    longitude_column: ClassVar[str] = "LONGITUDE"


# Instantiated once at startup in data_access_service.server
REPOSITORY_CLASSES: dict[str, type[ParquetRepository]] = {
    "mooring": MooringRepository,
    "wave-buoy": WaveBuoyRepository,
}


def build_repositories(session: ParquetDuckDBClient) -> dict[str, ParquetRepository]:
    """Instantiate one repository per product, all sharing one ``ParquetDuckDBClient``."""
    return {name: cls(session) for name, cls in REPOSITORY_CLASSES.items()}
