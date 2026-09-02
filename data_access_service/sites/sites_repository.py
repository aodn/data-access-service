"""A base Parquet repository over a DuckDB client.

``ParquetRepository`` binds one :class:`SitesDuckDBClient` to one Parquet
dataset — each dataset gets its own subclass below (:class:`MooringRepository`,
:class:`WaveBuoyRepository`) that declares where it lives and adds
dataset-specific reads.
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from typing import ClassVar

from data_access_service.config.config import Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.duckdbclient import SitesDuckDBClient


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
    (:class:`MooringRepository`, :class:`WaveBuoyRepository`, below). Subclasses
    set the location/schema attributes below; any subclass that omits a
    required one fails at import (see :meth:`__init_subclass__`). Because a
    repository is bound to a single table, none of the read methods take a
    table name.

    Location (set per dataset):
      ``bucket`` / ``dataset``                   — S3 bucket / Hive-partitioned prefix for the primary source (read only by the Batch job's :meth:`load`).
      ``snapshot_bucket`` / ``snapshot_dataset``  — S3 bucket / flat-file path for the snapshot the always-on service actually reads.
      ``table``                                   — name of the in-memory table the dataset is loaded into.

    Schema (default to the common IMOS timeseries names; override as needed):
      ``time_column``, ``site_column``, ``latitude_column``, ``longitude_column``.
    """

    table: ClassVar[str]
    dataset: ClassVar[str]
    bucket: ClassVar[str]
    snapshot_bucket: ClassVar[str]
    snapshot_dataset: ClassVar[str]
    time_column: ClassVar[str]
    site_column: ClassVar[str]
    latitude_column: ClassVar[str]
    longitude_column: ClassVar[str]
    value_columns: ClassVar[Sequence[str]]
    group_column: ClassVar[str | None] = None
    value_columns_quality_control_columns: ClassVar[Sequence[str]] = ()

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

    def __init__(self, session: SitesDuckDBClient) -> None:
        if type(self) is ParquetRepository:
            raise TypeError(
                "ParquetRepository is abstract; instantiate a dataset subclass"
            )
        self.session = session
        self._configure_s3()
        self._configure_snapshot_bucket_s3()
        self._loaded_snapshot_etag: str | None = None

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

    def _configure_snapshot_bucket_s3(self) -> None:
        """Create the S3 secret DuckDB uses to read the snapshot dataset."""
        self.session.create_s3_secret(self.snapshot_bucket)

    def load(self) -> ParquetRepository:
        """Materialize the PRIMARY dataset into this dataset's ``table``.

        Reads the Hive-partitioned ``dataset`` directory and replaces the table.
        Raises if the read fails; because ``CREATE OR REPLACE TABLE`` is atomic,
        a failed read rolls back and leaves any existing table intact. Returns
        ``self`` so callers can chain ``Repo(session).load()``.
        """
        cols = ", ".join(quote_ident(c) for c in self.load_columns)
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
        return self

    def load_snapshot(self) -> ParquetRepository:
        """Load the table from ``snapshot_dataset`` (a single flat Parquet file).

        The Batch job writes ``snapshot_dataset`` via :meth:`write_snapshot`;
        this reads it directly (it's one flat file, not a partitioned
        directory). Called by :meth:`reload_if_changed`, which only calls this
        once it has confirmed the snapshot exists.
        """
        self.session.execute(
            f"""
            CREATE OR REPLACE TABLE {quote_ident(self.table)} AS
            SELECT * FROM read_parquet('{self.snapshot_dataset}')
            """
        )
        return self

    def snapshot_etag(self) -> str | None:
        """Current S3 ETag of ``snapshot_dataset``, or ``None`` if it doesn't exist yet.

        A single S3 HEAD — no data transfer — used by :meth:`reload_if_changed`
        to detect a new snapshot before paying for a table rebuild.
        """
        key = self.snapshot_dataset.removeprefix(f"s3://{self.snapshot_bucket}/")
        aws = AWSHelper()
        try:
            response = aws.s3.head_object(Bucket=self.snapshot_bucket, Key=key)
        except aws.s3.exceptions.ClientError:
            return None
        return response["ETag"]

    def reload_if_changed(self) -> bool:
        """Reload the table from ``snapshot_dataset`` if its S3 ETag has changed.

        The always-on service's only way of picking up new data — called both
        at startup and on every recurring reload tick. Cheap either way: one
        HEAD, plus — only when changed — the lightweight :meth:`load_snapshot`
        read. Returns whether a reload happened.
        """
        etag = self.snapshot_etag()
        if etag is None or etag == self._loaded_snapshot_etag:
            return False
        self.load_snapshot()
        self._loaded_snapshot_etag = etag
        return True

    def write_snapshot(self) -> None:
        """Write the current table to ``snapshot_dataset`` as one Parquet file.

        Called after a successful primary :meth:`load` so the snapshot always
        mirrors the latest good data.
        """
        self.session.execute(
            f"""
            COPY (SELECT * FROM {quote_ident(self.table)})
            TO '{self.snapshot_dataset}' (FORMAT PARQUET)
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
    bucket: ClassVar[str] = config.get_sites_config().co_bucket
    snapshot_bucket: ClassVar[str] = config.get_mooring_snapshot_bucket_name()
    dataset: ClassVar[str] = f"s3://{bucket}/{table}.parquet"
    snapshot_dataset: ClassVar[str] = (
        f"s3://{snapshot_bucket}/imoslive/MOORING/{table}.parquet"
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
    bucket: ClassVar[str] = config.get_sites_config().co_bucket
    snapshot_bucket: ClassVar[str] = config.get_wave_buoy_snapshot_bucket_name()
    dataset: ClassVar[str] = f"s3://{bucket}/{table}.parquet"
    snapshot_dataset: ClassVar[str] = (
        f"s3://{snapshot_bucket}/imoslive/BUOY/{table}.parquet"
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


def build_repositories(session: SitesDuckDBClient) -> dict[str, ParquetRepository]:
    """Instantiate one repository per product, all sharing one ``SitesDuckDBClient``."""
    return {name: cls(session) for name, cls in REPOSITORY_CLASSES.items()}
