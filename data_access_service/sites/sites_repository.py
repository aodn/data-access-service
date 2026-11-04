"""Site timeseries repositories: one :class:`ParquetRepository` subclass per Parquet dataset.

Subclasses only declare config (location, columns, grouping). Register them in
:data:`REPOSITORY_CLASSES` to get the API endpoints and the Batch refresh.
"""

from __future__ import annotations

import logging
from abc import ABC
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import ClassVar

import duckdb
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from data_access_service.config.config import Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.duckdbclients.sitedbclient import SitesDuckDBClient
from data_access_service.utils.retry_utils import log_retry_attempt
from data_access_service.utils.sql_utils import quote_ident

logger = logging.getLogger(__name__)


_LOAD_RETRY_MIN_WAIT = timedelta(seconds=30)
_LOAD_RETRY_MAX_WAIT = timedelta(minutes=1)
_LOAD_RETRY_MAX_ATTEMPTS = 3

MAX_GROUP_COLUMNS = 2


class ParquetRepository(ABC):
    """Abstract base for one timeseries Parquet dataset; subclasses set the attributes below.

    Required (missing ones raise ``TypeError`` at import):
      ``bucket`` / ``dataset``                   — primary source, read by :meth:`load` (Batch).
      ``snapshot_bucket`` / ``snapshot_dataset``  — single-file snapshot the API reads.
      ``table``                                   — DuckDB table/view name.
      ``time_column``, ``site_column``, ``latitude_column``, ``longitude_column``,
      ``value_columns``.

    Optional:
      ``value_columns_quality_control_columns`` — QC column per value column, same order.
      ``group_columns``   — columns the details series nest by, outermost first.
      ``derived_columns`` — columns :meth:`load` computes, as ``{name: SQL expression}``.
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
    group_columns: ClassVar[Sequence[str]] = ()
    # TEMPORARY: the dataset has no instrument column yet. Once it does, drop this
    # derived_columns entry and group by that column directly.
    derived_columns: ClassVar[Mapping[str, str]] = {}
    value_columns_quality_control_columns: ClassVar[Sequence[str]] = ()

    def __init_subclass__(cls, **kwargs) -> None:
        """Fail at import if a required attribute (annotated here with no default) is missing,
        the QC columns don't line up with ``value_columns``, or there are too many
        ``group_columns``."""
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
        if len(cls.group_columns) > MAX_GROUP_COLUMNS:
            raise TypeError(
                f"{cls.__name__}.group_columns allows at most {MAX_GROUP_COLUMNS} "
                f"columns, got {len(cls.group_columns)}"
            )

    def __init__(self, session: SitesDuckDBClient) -> None:
        if type(self) is ParquetRepository:
            raise TypeError(
                "ParquetRepository is abstract; instantiate a dataset subclass"
            )
        self.session = session
        self.session.create_s3_secret(self.bucket)
        self.session.create_s3_secret(self.snapshot_bucket)
        self._loaded_snapshot_etag: str | None = None

    @property
    def value_quality_control_map(self) -> dict[str, str]:
        """``{value column: QC column}``, paired by position; empty without QC columns."""
        if not self.value_columns_quality_control_columns:
            return {}
        return dict(zip(self.value_columns, self.value_columns_quality_control_columns))

    @property
    def load_columns(self) -> list[str]:
        """Columns :meth:`load` keeps: time, site, location, group, value and QC, deduplicated.

        Any of these named in ``derived_columns`` is computed instead of read.
        """
        cols = [
            self.time_column,
            self.site_column,
            self.latitude_column,
            self.longitude_column,
        ]
        cols += [c for c in self.group_columns if c not in cols]
        cols += [c for c in self.value_columns if c not in cols]
        cols += [c for c in self.value_columns_quality_control_columns if c not in cols]
        return cols

    # Bug in tenacity, the type check always fail but function ok
    # noinspection PyCallingNonCallable
    @retry(
        stop=stop_after_attempt(_LOAD_RETRY_MAX_ATTEMPTS),
        wait=wait_exponential(
            multiplier=_LOAD_RETRY_MIN_WAIT.total_seconds(),
            min=_LOAD_RETRY_MIN_WAIT,
            max=_LOAD_RETRY_MAX_WAIT,
        ),
        retry=retry_if_exception_type((duckdb.IOException, UnicodeDecodeError)),
        before_sleep=log_retry_attempt("SitesRepository.load()", logger),
        reraise=True,
    )
    # wait = multiplier * exp_base**(attempt_number - 1), clamped to [min, max].
    def load(self) -> ParquetRepository:
        """Read the primary ``dataset`` into ``table`` (Batch job only). Returns ``self``.

        Retries on ``IOException`` / ``UnicodeDecodeError``, which happen when the
        producer is rewriting the source. A failed load leaves the old table intact.
        """
        cols = ", ".join(
            (
                f"{self.derived_columns[c]} AS {quote_ident(c)}"
                if c in self.derived_columns
                else quote_ident(c)
            )
            for c in self.load_columns
        )
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
        """Point ``table`` at ``snapshot_dataset`` as a VIEW (API service).

        A view, not a table, so the API doesn't copy the snapshot into memory;
        filters and projections push down into the Parquet scan.
        """
        self.session.execute(
            f"""
            CREATE OR REPLACE VIEW {quote_ident(self.table)} AS
            SELECT * FROM read_parquet('{self.snapshot_dataset}')
            """
        )
        return self

    def snapshot_etag(self) -> str | None:
        """S3 ETag of ``snapshot_dataset`` (one HEAD), or ``None`` if it doesn't exist."""
        key = self.snapshot_dataset.removeprefix(f"s3://{self.snapshot_bucket}/")
        aws = AWSHelper()
        try:
            response = aws.s3.head_object(Bucket=self.snapshot_bucket, Key=key)
        except aws.s3.exceptions.ClientError:
            return None
        return response["ETag"]

    def reload_if_changed(self) -> bool:
        """Call :meth:`load_snapshot` if the snapshot's ETag changed; return whether it did.

        How the API picks up new data, at startup and on each scheduler tick.
        """
        etag = self.snapshot_etag()
        if etag is None or etag == self._loaded_snapshot_etag:
            return False
        self.load_snapshot()
        self._loaded_snapshot_etag = etag
        return True

    def write_snapshot(self) -> None:
        """Write ``table`` to ``snapshot_dataset`` as one Parquet file, after :meth:`load`.

        Sorted by site, group columns, then time, so each site sits in a few row
        groups and :meth:`site_details` can skip the rest of the file.
        """
        order_columns = [self.site_column, *self.group_columns, self.time_column]
        order_by = ", ".join(quote_ident(c) for c in order_columns)

        self.session.execute(
            f"""
            COPY (SELECT * FROM {quote_ident(self.table)} ORDER BY {order_by})
            TO '{self.snapshot_dataset}' (FORMAT PARQUET)
            """
        )

    def is_loaded(self) -> bool:
        """Whether ``table`` exists yet (table or snapshot view); routes return 503 until then."""
        row = self.session.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [self.table],
        ).fetchone()
        return row is not None

    def sites_in_date_range(self, start: str | None = None, end: str | None = None):
        """One row per site: location and time of its latest record within ``[start, end]``.

        ``start`` / ``end`` are optional and inclusive.
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
        """Most recent time across all rows."""
        (value,) = self.session.execute(
            f"SELECT max({quote_ident(self.time_column)}) "
            f"FROM {quote_ident(self.table)}"
        ).fetchone()
        return value

    def site_details(self, site: str, start: str | None = None, end: str | None = None):
        """One site's rows in ``[start, end]`` (optional, inclusive), sorted by group then time.

        Returns time, location, group and value columns. A value whose QC flag
        isn't 1 is set to NULL; the rest of the row is kept.
        """
        required = [self.time_column, self.latitude_column, self.longitude_column]
        required += [c for c in self.group_columns if c not in required]
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
        order = ", ".join(
            quote_ident(c) for c in [*self.group_columns, self.time_column]
        )

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
    """Mooring timeseries (realtime QC), grouped by depth."""

    config: Config = Config.get_config()

    table: ClassVar[str] = "mooring_timeseries_realtime_qc"
    bucket: ClassVar[str] = config.get_sites_config().co_bucket
    snapshot_bucket: ClassVar[str] = config.get_datavis_data_bucket_name()
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
    group_columns: ClassVar[tuple[str, ...]] = ("NOMINAL_DEPTH",)
    time_column: ClassVar[str] = "TIME"
    site_column: ClassVar[str] = "site_code"
    latitude_column: ClassVar[str] = "LATITUDE"
    longitude_column: ClassVar[str] = "LONGITUDE"


class WaveBuoyRepository(ParquetRepository):
    """Wave buoy (realtime, no QC), ungrouped."""

    config: Config = Config.get_config()

    table: ClassVar[str] = "wave_buoy_realtime_nonqc"
    bucket: ClassVar[str] = config.get_sites_config().co_bucket
    snapshot_bucket: ClassVar[str] = config.get_datavis_data_bucket_name()
    dataset: ClassVar[str] = f"s3://{bucket}/{table}.parquet"
    snapshot_dataset: ClassVar[str] = (
        f"s3://{snapshot_bucket}/imoslive/BUOY/{table}.parquet"
    )
    value_columns: ClassVar[tuple[str, ...]] = ("WSSH", "SSWMD", "WPFM", "WPMH", "WHTH")
    time_column: ClassVar[str] = "TIME"
    site_column: ClassVar[str] = "site_name"
    latitude_column: ClassVar[str] = "LATITUDE"
    longitude_column: ClassVar[str] = "LONGITUDE"


class MooringEstuarineWaterQualityRepository(ParquetRepository):
    """Estuarine/coastal water-quality moorings (realtime QC), grouped by instrument then depth."""

    config: Config = Config.get_config()

    table: ClassVar[str] = (
        "mooring_estuarine_coastal_water_quality_monitoring_realtime_qc"
    )
    bucket: ClassVar[str] = config.get_sites_config().co_bucket
    snapshot_bucket: ClassVar[str] = config.get_datavis_data_bucket_name()
    dataset: ClassVar[str] = f"s3://{bucket}/{table}.parquet"
    snapshot_dataset: ClassVar[str] = (
        f"s3://{snapshot_bucket}/imoslive/MOORING_ESTUARINE_WATER_QUALITY/{table}.parquet"
    )
    value_columns: ClassVar[tuple[str, ...]] = (
        "TEMP",
        "PSAL",
        "CPHL",
        "DOXS",
        "FDOM",
        "PHYCO",
        "TURB",
        "FLUSI",
    )
    # QARTOD flags: 1 pass, 2 not evaluated, 3 questionable, 4 bad, 9 missing
    value_columns_quality_control_columns: ClassVar[tuple[str, ...]] = (
        "TEMP_quality_control",
        "PSAL_quality_control",
        "CPHL_quality_control",
        "DOXS_quality_control",
        "FDOM_quality_control",
        "PHYCO_quality_control",
        "TURB_quality_control",
        "FLUSI_quality_control",
    )
    # SA sites have two instruments at one depth; the instrument is only in the filename,
    # e.g. "..._FV00_SACM-BOS-20260220_EXO2-SONDE.nc" -> "EXO2-SONDE".
    # TEMPORARY: the dataset has no instrument column yet. Once it does, drop this
    # derived_columns entry and group by that column directly.
    derived_columns: ClassVar[dict[str, str]] = {
        "instrument": (
            "coalesce(nullif(regexp_extract(filename, '_FV0[0-9]_[^_]+_(.+)\\.nc$', 1), ''), "
            "'unknown')"
        )
    }
    group_columns: ClassVar[tuple[str, ...]] = ("instrument", "NOMINAL_DEPTH")
    time_column: ClassVar[str] = "TIME"
    site_column: ClassVar[str] = "site_code"
    latitude_column: ClassVar[str] = "LATITUDE"
    longitude_column: ClassVar[str] = "LONGITUDE"


# Product key -> repository. Used by the API routes and the Batch refresh.
REPOSITORY_CLASSES: dict[str, type[ParquetRepository]] = {
    "mooring": MooringRepository,
    "wave-buoy": WaveBuoyRepository,
    "mooring-estuarine-water-quality": MooringEstuarineWaterQualityRepository,
}


def build_repositories(session: SitesDuckDBClient) -> dict[str, ParquetRepository]:
    """One repository per product, sharing ``session``."""
    return {name: cls(session) for name, cls in REPOSITORY_CLASSES.items()}
