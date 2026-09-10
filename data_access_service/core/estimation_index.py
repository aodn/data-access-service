"""Answer estimate_size for parquet keys from the pre-built index.

The index (built by ``batch/estimation``) turns the row count into one small
DuckDB query over a few-MB file, instead of listing the dataset and reading up
to 256 parquet footers on S3. The sidecar next to it carries the numbers that
are one per dataset: measured CSV bytes per row, measured zip ratio, and the
date range the index covers.

Everything here fails soft. When there is no index, when it is too old to
trust, or when the query errors, the caller falls back to the live scan in
``size_estimation._estimate_parquet_size`` - an estimate must never fail
because of this optimisation.

The parquet download also uses the index for cheap time-window row counts
when splitting jobs (``parquet_date_ranges.check_rows_with_date_range``).
Days after ``max_date`` are not in the file; that tail must not be treated
as empty.
"""

import json
import logging
import math
import threading
import time
from typing import Callable, List, Optional, Tuple

import pandas as pd

from data_access_service.config.config import Config
from data_access_service.core.duckdbclient import EstimationDuckDBClient
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.estimation_types import (
    ESTIMATION_INDEX_VERSION,
    EstimationSidecarMetadata,
    schema_fingerprint,
)
from data_access_service.utils.date_time_utils import ensure_timezone
from data_access_service.utils.format_utils import OUTPUT_FORMAT_CSV

log = logging.getLogger(__name__)

# (uuid, key) -> sidecar, refreshed this often. Also caches "no index" so a
# dataset that was never built does not cost an S3 GET on every request.
_SIDECAR_TTL_SECONDS = 60 * 60

# How many covered days at the end of the index are averaged to extrapolate the
# days the index does not reach yet (see _extrapolate_tail).
_TAIL_SAMPLE_DAYS = 30

_sidecar_cache: dict[
    tuple[str, str], tuple[float, Optional[EstimationSidecarMetadata]]
] = {}
_sidecar_lock = threading.Lock()

# DuckDB client used to read the index. This module owns it: an
# EstimationDuckDBClient on its own :memory: database, built on first use (or
# eagerly by the server lifespan via init_client). set_duckdb_client stays for
# tests that want to hand in a stub. It used to be the app-level sites client,
# which coupled the estimate to the sites tuning and the sites class name.
_duckdb_client: Optional[EstimationDuckDBClient] = None
_owned_client: Optional[EstimationDuckDBClient] = None
_client_lock = threading.Lock()
# Buckets a given client already has S3 credentials for.
_secret_done: set[tuple[int, str]] = set()

# Failures that mean this module is wrong, not that the index is missing or S3
# is unhappy. They are logged at ERROR rather than folded into the ordinary
# fail-soft WARNING, so a refactor cannot quietly turn the index off.
_BUG_ERRORS = (ImportError, AttributeError, NameError, TypeError)


ExtentProvider = Callable[
    [str, str], Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]
]


def set_duckdb_client(client: Optional[EstimationDuckDBClient]) -> None:
    """Override the client this module uses (tests hand in a stub)."""
    global _duckdb_client
    with _client_lock:
        _duckdb_client = client


def clear_sidecar_cache() -> None:
    """Forget every cached sidecar (used after a rebuild)."""
    with _sidecar_lock:
        _sidecar_cache.clear()


def read_index_estimate(
    api,
    uuid: str,
    key: str,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    bboxes: List[BoundingBox],
    output_format: str,
    columns: Optional[List[str]] = None,
    requested_end_date: Optional[pd.Timestamp] = None,
) -> Optional[dict]:
    """Estimate one parquet key from the index.

    :param date_start/date_end: the range already trimmed to this key's extent
    :param requested_end_date: the end date the user actually asked for, before
        any trim. Days between the index's last covered day and this are
        extrapolated rather than dropped (see _extrapolate_tail)
    :return: the same dict shape ``_estimate_parquet_size`` returns, or None
        when the index cannot be used and the caller must do the live scan
    """
    meta = usable_sidecar(api, uuid, key, output_format)
    if meta is None:
        return None

    try:
        client = _get_client()
        _ensure_secret(client)
        path = index_s3_path(uuid, key)

        rows = _count_rows(client, path, meta, date_start, date_end, bboxes)
        extra_rows, tail_note = _extrapolate_tail(
            client, path, meta, date_start, bboxes, requested_end_date
        )
    except _BUG_ERRORS as e:
        # Not a data or S3 problem - this module is broken. Still fall back
        # (an estimate must never fail because of this optimisation), but say
        # so at ERROR: the fallback is 50x slower, so a silent WARNING here is
        # how a rename went unnoticed in production once already.
        log.error(
            "estimation index is broken for %s/%s - falling back to the live "
            "scan, which is far slower. This is a code fault, not a missing "
            "index: %s",
            uuid,
            key,
            e,
            exc_info=True,
        )
        return None
    except Exception as e:
        log.warning(
            "estimation index query failed for %s/%s; falling back to the live "
            "scan: %s",
            uuid,
            key,
            e,
        )
        return None

    total_rows = rows + extra_rows
    total_uncompressed = int(
        total_rows * meta.csv_bytes_per_row
        + (meta.csv_header_bytes if total_rows else 0)
    )
    total_output = int(total_uncompressed * meta.zip_ratio)

    notes: list[str] = ["estimated from the pre-built index"]
    if len(bboxes) > 1:
        notes.append(f"union of {len(bboxes)} polygon bboxes")
    if columns:
        # Same as the download: query_data passes no columns either, so the CSV
        # always carries every column.
        notes.append(f"column subsetting not supported yet; columns skipped: {columns}")
    if bboxes:
        notes.append(
            f"bbox upper bound: cells of {meta.effective_bin_size:g} deg that "
            "only partly overlap the requested area are counted whole"
        )
    if meta.has_time:
        notes.append(
            "day granularity: a day the request only partly covers is counted "
            "whole (upper bound)"
        )
    if tail_note:
        notes.append(tail_note)
    notes.append(
        f"~{total_rows:,} rows, ~{meta.csv_bytes_per_row:,.1f} CSV bytes per row "
        f"and zip ratio {meta.zip_ratio} measured from the dataset itself"
    )
    notes.append(
        f"estimated download size ~{round(total_output / 1_000_000, 1)} MB "
        f"(uncompressed ~{round(total_uncompressed / 1_000_000, 1)} MB)"
    )

    log.debug(
        "read_index_estimate: uuid=%s key=%s rows=%d (+%d extrapolated) "
        "uncompressed=%d output=%d",
        uuid,
        key,
        rows,
        extra_rows,
        total_uncompressed,
        total_output,
    )

    return {
        "uuid": uuid,
        "key": key,
        "format": output_format,
        "estimated_uncompressed_bytes": total_uncompressed,
        "estimated_output_bytes": total_output,
        "notes": "; ".join(dict.fromkeys(notes)),
    }


def sidecar_extent_provider(api) -> ExtentProvider:
    """A temporal-extent lookup backed by the index sidecar.

    Only the estimation path uses this. The download keeps calling
    ``api.get_temporal_extent`` (a real scan of the live data), because it
    produces the actual file and cannot be built from a weekly snapshot.

    Falls back to the real scan for any key with no usable index.
    """

    def provider(
        uuid: str, key: str
    ) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        meta = usable_sidecar(api, uuid, key, OUTPUT_FORMAT_CSV)
        # A timeless dataset stores a synthetic day key, which says nothing
        # about the data's real extent.
        if meta is None or not meta.has_time:
            return api.get_temporal_extent(uuid, key)
        try:
            return _sidecar_extent(meta)
        except Exception as e:
            log.warning(
                "sidecar extent unusable for %s/%s; using the live scan: %s",
                uuid,
                key,
                e,
            )
            return api.get_temporal_extent(uuid, key)

    return provider


def usable_sidecar(
    api, uuid: str, key: str, output_format: str
) -> Optional[EstimationSidecarMetadata]:
    """The sidecar for this key, or None when the index must not be used.

    Rejects (and logs the reason): the index switch off, a non-csv format, a
    missing sidecar, a version this build does not know, and a column set that
    changed since the build - which is what makes csv_bytes_per_row stale.
    """
    if output_format != OUTPUT_FORMAT_CSV:
        # The index only models the zipped-CSV download.
        return None
    return sidecar_for_row_counts(api, uuid, key)


def sidecar_for_row_counts(
    api, uuid: str, key: str
) -> Optional[EstimationSidecarMetadata]:
    """Sidecar if the index can answer a time-window row count.

    Same validity checks as :func:`usable_sidecar` (switch, version, schema)
    but ignores output format: the download path only needs ``SUM(c)``.
    """
    estimation_config = Config.get_config().get_estimation_config()
    if not estimation_config.use_index_for_estimate:
        return None
    if not key.endswith(".parquet"):
        return None

    meta = load_sidecar(uuid, key)
    if meta is None:
        return None

    if meta.version != ESTIMATION_INDEX_VERSION:
        log.info(
            "estimation index for %s/%s is version %s, this build reads %s; "
            "using the live scan",
            uuid,
            key,
            meta.version,
            ESTIMATION_INDEX_VERSION,
        )
        return None

    live_fingerprint = _live_schema_fingerprint(api, uuid, key)
    # An empty fingerprint on either side means "cannot compare", not "differs".
    if live_fingerprint and meta.schema_fingerprint:
        if live_fingerprint != meta.schema_fingerprint:
            log.info(
                "estimation index for %s/%s was built for a different column "
                "set; using the live scan",
                uuid,
                key,
            )
            return None

    return meta


def index_coverage_end(meta: EstimationSidecarMetadata) -> pd.Timestamp:
    """Last nanosecond of the last day the index covers (not clamped to now)."""
    # Day-resolution timestamps silently drop nanosecond=999 in replace();
    # force ns first (same trap as date_time_utils._end_of_day_nano).
    return (
        _day_key_to_timestamp(meta.max_date)
        .as_unit("ns")
        .replace(hour=23, minute=59, second=59, microsecond=999999, nanosecond=999)
    )


def count_index_rows(
    uuid: str,
    key: str,
    meta: EstimationSidecarMetadata,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    bboxes: Optional[List[BoundingBox]] = None,
) -> Optional[int]:
    """``SUM(c)`` over the day range and optional bbox(es).

    Returns None when the query fails so the caller can live-count. Does not
    extrapolate days after ``max_date``: those rows are absent from the SUM
    and must not be treated as a real zero. A bbox is an upper bound (whole
    cells that only partly overlap the box are counted).
    """
    try:
        client = _get_client()
        _ensure_secret(client)
        path = index_s3_path(uuid, key)
        return _count_rows(
            client, path, meta, date_start, date_end, bboxes=bboxes or []
        )
    except _BUG_ERRORS as e:
        log.error(
            "estimation index is broken for %s/%s row count - falling back "
            "to the live scan: %s",
            uuid,
            key,
            e,
            exc_info=True,
        )
        return None
    except Exception as e:
        log.warning(
            "estimation index row count failed for %s/%s; falling back to "
            "the live scan: %s",
            uuid,
            key,
            e,
        )
        return None


def load_sidecar(uuid: str, key: str) -> Optional[EstimationSidecarMetadata]:
    """Read ``{key}.metadata`` from S3, cached for a few minutes.

    Misses are cached too, so a dataset with no index costs one S3 GET per TTL
    rather than one per request.
    """
    cache_key = (uuid, key)
    now = time.monotonic()
    with _sidecar_lock:
        cached = _sidecar_cache.get(cache_key)
        if cached is not None and cached[0] > now:
            return cached[1]

    # Imported here, not at module import time: core.api imports this module,
    # and AWSHelper imports the package that defines core.api.
    from data_access_service.core.AWSHelper import AWSHelper

    estimation_config = Config.get_config().get_estimation_config()
    s3_key = f"{estimation_config.s3_prefix}/{uuid}/{key}.metadata"
    meta: Optional[EstimationSidecarMetadata] = None
    try:
        raw = AWSHelper().get_s3_object(estimation_config.bucket_name, s3_key)
        if raw is not None:
            meta = EstimationSidecarMetadata.from_dict(json.loads(raw.decode("utf-8")))
    except Exception as e:
        # Includes the 404 some clients raise instead of returning None.
        log.debug("no usable estimation sidecar at %s: %s", s3_key, e)
        meta = None

    with _sidecar_lock:
        _sidecar_cache[cache_key] = (now + _SIDECAR_TTL_SECONDS, meta)
    return meta


def index_s3_path(uuid: str, key: str) -> str:
    estimation_config = Config.get_config().get_estimation_config()
    return (
        f"s3://{estimation_config.bucket_name}/"
        f"{estimation_config.s3_prefix}/{uuid}/{key}.parquet"
    )


# ----------------------------------------------------------------------
# Query
# ----------------------------------------------------------------------


def _count_rows(
    client: EstimationDuckDBClient,
    path: str,
    meta: EstimationSidecarMetadata,
    date_start: Optional[pd.Timestamp],
    date_end: Optional[pd.Timestamp],
    bboxes: List[BoundingBox],
) -> int:
    """SUM(c) over the day range and the bbox(es), in one query.

    Several bboxes are one OR chain, so a cell inside two overlapping boxes is
    counted once - the live scan has to dedupe fragments by path to get that.
    """
    where, params = _where_clause(meta, date_start, date_end, bboxes)
    sql = f"SELECT COALESCE(SUM(c), 0)::BIGINT FROM read_parquet('{path}')"
    if where:
        sql += " WHERE " + where
    row = client.execute(sql, params).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _where_clause(
    meta: EstimationSidecarMetadata,
    date_start: Optional[pd.Timestamp],
    date_end: Optional[pd.Timestamp],
    bboxes: List[BoundingBox],
) -> tuple[str, list]:
    clauses: list[str] = []
    params: list = []

    # A timeless dataset holds one synthetic day key, so filtering on dates
    # would drop everything. The download does not filter it either.
    if meta.has_time and date_start is not None and date_end is not None:
        clauses.append("d BETWEEN ? AND ?")
        params += [_day_key(date_start), _day_key(date_end)]

    if bboxes:
        # With a bbox, rows with a NULL bin drop out through BETWEEN - which
        # matches the download, where a row with no position cannot pass the
        # polygon filter. With no bbox they stay in, as they should.
        ors = []
        for bbox in bboxes:
            ors.append("(lat_bin BETWEEN ? AND ? AND lon_bin BETWEEN ? AND ?)")
            params += [
                _bin(bbox.min_lat, meta),
                _bin(bbox.max_lat, meta),
                _bin(bbox.min_lon, meta),
                _bin(bbox.max_lon, meta),
            ]
        clauses.append("(" + " OR ".join(ors) + ")")

    return " AND ".join(clauses), params


def _extrapolate_tail(
    client: EstimationDuckDBClient,
    path: str,
    meta: EstimationSidecarMetadata,
    date_start: Optional[pd.Timestamp],
    bboxes: List[BoundingBox],
    requested_end_date: Optional[pd.Timestamp],
) -> tuple[int, Optional[str]]:
    """Rows for the days the index does not reach yet.

    The index is a weekly snapshot, so a request that runs past its last
    covered day would otherwise be silently short - and an estimate that is too
    small is worse than a rough one, because the user then gets a much bigger
    download than promised. The uncovered days are charged at the average of
    the last covered days.
    """
    if not meta.has_time or requested_end_date is None:
        return 0, None

    requested_end = ensure_timezone(requested_end_date)
    now = pd.Timestamp.now(tz="UTC")
    if requested_end > now:
        requested_end = now

    last_covered = _day_key_to_timestamp(meta.max_date)
    uncovered_days = (requested_end.normalize() - last_covered.normalize()).days
    if uncovered_days <= 0:
        return 0, None

    # Average over the last covered days, inside the same area, so a request
    # over a small box is not charged the whole dataset's daily rate.
    window_start = last_covered - pd.Timedelta(days=_TAIL_SAMPLE_DAYS - 1)
    if date_start is not None:
        window_start = max(window_start, ensure_timezone(date_start))
    window_days = (last_covered.normalize() - window_start.normalize()).days + 1
    if window_days <= 0:
        return 0, None

    window_rows = _count_rows(client, path, meta, window_start, last_covered, bboxes)
    rows_per_day = window_rows / window_days
    extra_rows = int(round(rows_per_day * uncovered_days))
    if extra_rows <= 0:
        return 0, None

    note = (
        f"index covers up to {meta.max_date}; {uncovered_days} later day(s) "
        f"extrapolated from the {window_days} day(s) before it"
    )
    return extra_rows, note


def _sidecar_extent(
    meta: EstimationSidecarMetadata,
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """The sidecar's day range in the exact shape ``get_temporal_extent`` returns.

    Start at 00:00:00, end at the last nanosecond of the day, end clamped to
    now. Skipping any of this would make the estimate use a different window
    than the download.
    """
    start = _day_key_to_timestamp(meta.min_date)
    end = index_coverage_end(meta)
    now = pd.Timestamp.now(tz="UTC")
    if end > now:
        end = now
    return start, end


def _day_key(ts: pd.Timestamp) -> int:
    """UTC YYYYMMDD, the same key the index is built with."""
    ts = ensure_timezone(ts).tz_convert("UTC")
    return ts.year * 10000 + ts.month * 100 + ts.day


def _day_key_to_timestamp(day_key: int) -> pd.Timestamp:
    return pd.Timestamp(str(int(day_key)), tz="UTC")


def _bin(degrees: float, meta: EstimationSidecarMetadata) -> int:
    """The cell a coordinate falls in, computed exactly as the build computes it.

    Two floors in the same order, never one floor by the product: the index is
    built as floor(x / bin_size), then whole cells are merged by
    floor(cell / merge_factor). The identity
    floor(floor(x/a)/n) == floor(x/(a*n)) is exact in real arithmetic, but a
    bin size like 0.1 is not exactly representable, so taking the shortcut
    disagrees with the build by one cell on exact cell boundaries - which can
    drop a cell that is inside the requested box.
    """
    cell = math.floor(float(degrees) / meta.bin_size)
    if meta.bin_merge_factor > 1:
        cell = math.floor(cell / meta.bin_merge_factor)
    return int(cell)


def _live_schema_fingerprint(api, uuid: str, key: str) -> str:
    get_variables = getattr(api, "get_dataset_variables", None)
    if not callable(get_variables):
        return ""
    try:
        return schema_fingerprint(get_variables().get(uuid, {}).get(key))
    except Exception:
        return ""


def _get_client() -> EstimationDuckDBClient:
    """This module's own client, built on first use (or by :func:`init_client`)."""
    global _owned_client
    with _client_lock:
        if _duckdb_client is not None:
            return _duckdb_client
        if _owned_client is None:
            _owned_client = EstimationDuckDBClient(
                Config.get_config().get_estimation_config().read_duckdb
            )
        return _owned_client


def init_client() -> None:
    """Build the client now, so a broken read path fails at startup.

    The server calls this from its lifespan. The point is not the few
    milliseconds it saves on the first estimate - it is that a refactor which
    breaks this module surfaces as a failed deploy rather than as an estimate
    that silently falls back to the live scan and gets 50x slower.
    """
    _get_client()


def close_client() -> None:
    """Close the client this module owns (called from the lifespan shutdown)."""
    global _owned_client
    with _client_lock:
        client = _owned_client
        _owned_client = None
    if client is not None:
        client.close()


def _ensure_secret(client: EstimationDuckDBClient) -> None:
    """Give the client S3 credentials for the portal-data bucket, once.

    The app-level client is only given secrets for the buckets the site
    repositories read, and the index lives in a different bucket.
    """
    bucket = Config.get_config().get_estimation_config().bucket_name
    marker = (id(client), bucket)
    with _client_lock:
        if marker in _secret_done:
            return
    client.create_s3_secret(bucket)
    with _client_lock:
        _secret_done.add(marker)
