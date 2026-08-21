"""Per-URL registry of long-lived Zarr handles via aodn_cloud_optimised.

Not a cache in the strict sense: handles are not evicted (the URL set is small
and bounded by registered products), and ``ttl`` triggers a background refresh
rather than expiry. Stale entries keep serving until the refresh completes, so
requests never block on freshness — only the very first open per URL blocks.

A per-store ``{timestamp: (raw_timestamp, iso_string)}`` index is built
alongside the source so ``load_slice`` can resolve a requested timestamp in
O(1) instead of scanning every timestamp on the hot path. ``iso_string`` is
formatted once here, at store open/refresh time, rather than once per
``/manifest`` request — ``get_available_dates`` just reshapes this same index
into a list. The tiler addresses data by exact UTC instant, not by calendar
day — see ``tiler/technical.md`` §9.

Single source of truth is the lib ``ZarrDataSource`` (opened with
``chunks=None`` so dask is not built at open time; native coord names for
``get_data``). Callers that need ``time``/``lat``/``lon`` use ``get_store``,
which derives a normalised view on demand.

``prewarm`` also decides whether a store is fit to serve tiler requests at
all (grid shape, time dimension) and records the verdict in ``_failed_stores``
— ``is_available`` is a plain membership check against it, meant to gate
requests before they ever reach ``get_store``/``load_slice``. A store only
leaves that set on a later successful prewarm; there is no per-request
self-heal for a store once prewarm has marked it failed (that is a cron job's
job, not a request's).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import random
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import anyio
import pandas as pd
import xarray as xr
from aodn_cloud_optimised.lib import DataQuery

from data_access_service.config.config import Config
from data_access_service.config.tiler.constants import COORD_NAMES
from data_access_service.tiler.utils.dates import ts_to_utc_iso

if TYPE_CHECKING:
    from aodn_cloud_optimised.lib.DataQuery import ZarrDataSource

logger = logging.getLogger(__name__)

_tiler_config = Config.get_config().get_tiler_config()

_STORE_TTL = float(_tiler_config.store_ttl_seconds)

# Capacity gate for concurrent store opens during prewarm. Bounded to the S3
# connection ceiling, not CPU. Runs on the shared anyio pool but a separate
# budget so a many-product startup can't transiently consume tile-handler slots.
_STORE_PREWARM_LIMITER = anyio.CapacityLimiter(_tiler_config.store_prewarm_workers)

# Bounded: a raw thread per expired store is a stampede at 60 stores.
_REFRESH_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=_tiler_config.store_refresh_workers,
    thread_name_prefix="store-refresh",
)

# So stores opened together at startup do not all expire at once.
_REFRESH_JITTER_FRACTION = 0.1

_PREWARM_MAX_ATTEMPTS = 3
_PREWARM_BACKOFF_SECONDS = 1.0


class NotGriddedStoreError(ValueError):
    """The store opened but is not a lat/lon grid. Retrying will not change it."""


class NoTimeDimensionError(ValueError):
    """The store opened but has no time dimension; every date request would 404."""


def _dataset_key_from_url(store_url: str) -> str:
    """Map a product ``source_path`` to the lib dataset key (``name.zarr``).

    Examples:
      ``s3://aodn-cloud-optimised/foo.zarr/`` → ``foo.zarr``
      ``s3://bucket/prefix/foo.zarr`` → ``foo.zarr``
    """
    path = urlparse(store_url).path if "://" in store_url else store_url
    key = path.rstrip("/").rsplit("/", 1)[-1]
    if not key.endswith(".zarr"):
        raise ValueError(
            f"Cannot derive dataset key from store URL {store_url!r} "
            f"(expected a path ending in '.zarr')"
        )
    return key


def _normalise_coords(ds: xr.Dataset, store_url: str) -> xr.Dataset:
    """Rename TIME/LATITUDE/LONGITUDE → time/lat/lon and validate dims.

    Every caller of ``_ensure_open`` — prewarm and lazy opens alike — needs a
    grid with a time axis, so both checks live here rather than being
    re-derived per caller.
    """
    rename = {k: v for k, v in COORD_NAMES.items() if k in ds.dims or k in ds.coords}
    if rename:
        ds = ds.rename(rename)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise NotGriddedStoreError(
            f"Store {store_url!r} missing lat/lon dims after rename (found: {list(ds.dims)})"
        )
    if "time" not in ds.dims:
        raise NoTimeDimensionError(
            f"Store {store_url!r} has no time dimension; every date request would 404"
        )
    return ds.sortby("time")


def _resolve_zarr_source(store_url: str) -> ZarrDataSource:
    """Open a ZarrDataSource via aodn_cloud_optimised with dask disabled.

    ``chunks=None`` is required: every tiler read is a single-slice
    ``get_data`` + eager ``.compute()``, so dask's task graph buys nothing
    while costing open-time memory on finely-chunked production stores
    (10M+ graph tasks / tens of GB just to describe layout). xarray still
    indexes Zarr lazily and only fetches native chunks on ``.compute()``.
    """
    key = _dataset_key_from_url(store_url)
    source = DataQuery.GetAodn().get_dataset(key, chunks=None)
    if not isinstance(source, DataQuery.ZarrDataSource):
        raise TypeError(
            f"Expected ZarrDataSource for {key!r}, got {type(source).__name__}"
        )
    return source


def _build_time_index(ds: xr.Dataset) -> dict[pd.Timestamp, tuple[object, str]]:
    """Return {timestamp: (raw_timestamp, iso_string)} for the dataset's time
    coord, or {}.
    """
    if "time" not in ds.dims:
        return {}
    times = ds.coords["time"].values
    if len(times) == 0:
        return {}
    return {pd.Timestamp(ts): (ts, ts_to_utc_iso(ts)) for ts in times}


def _open_store(store_url: str) -> ZarrDataSource:
    """Resolve via lib and normalise its dataset in place to time/lat/lon."""
    source = _resolve_zarr_source(store_url)
    # `source.zarr_store` already holds the full dataset (native TIME/LATITUDE/
    # LONGITUDE names) the moment the lib opens it; this just overwrites that
    # same attribute with the renamed/sorted view, once, so every later reader
    # (ours and the lib's own get_data) sees time/lat/lon without recomputing it.
    source.zarr_store = _normalise_coords(source.zarr_store, store_url)
    return source


class StoreRegistry:
    """See module docstring for the design.

    Concurrent first-time opens of the *same* URL share one open call via a
    per-URL ``concurrent.futures.Future``; opens of *different* URLs run in
    parallel (the original implementation serialised them under a single global
    lock until this pattern was introduced).
    """

    def __init__(self, ttl: float) -> None:
        self._ttl = ttl
        self._stores: dict[str, ZarrDataSource] = {}
        self._opened_at: dict[str, float] = {}
        self._ttl_jitter: dict[str, float] = {}
        self._refreshing: set[str] = set()
        self._in_flight: dict[str, concurrent.futures.Future] = {}
        self._time_index: dict[str, dict[pd.Timestamp, tuple[object, str]]] = {}
        self._failed_stores: dict[str, BaseException] = {}
        self._lock = threading.Lock()

    def _ensure_open(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived source for ``store_url``, opening on first request."""
        should_open = False
        with self._lock:
            if store_url in self._stores:
                deadline = self._ttl + self._ttl_jitter.get(store_url, 0.0)
                if time.monotonic() - self._opened_at[store_url] < deadline:
                    return self._stores[store_url]
                # Serve stale, refresh once in the background.
                if store_url not in self._refreshing:
                    self._refreshing.add(store_url)
                    logger.info(
                        f"Store TTL expired, refreshing in background: {store_url}"
                    )
                    _REFRESH_EXECUTOR.submit(self._refresh_background, store_url)
                return self._stores[store_url]
            if store_url in self._in_flight:
                future = self._in_flight[store_url]
            else:
                future = concurrent.futures.Future()
                self._in_flight[store_url] = future
                should_open = True

        if not should_open:
            return future.result()

        try:
            source = _open_store(store_url)
            index = _build_time_index(source.zarr_store)
            self._publish(store_url, source, index)
            logger.info(f"Store opened: {store_url} (timestamp_count={len(index)})")
            future.set_result(source)
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            with self._lock:
                self._in_flight.pop(store_url, None)
        return source

    def get(self, store_url: str) -> xr.Dataset:
        """Return a normalised (time/lat/lon) view, opening the source if needed."""
        return self._ensure_open(store_url).zarr_store

    def get_datasource(self, store_url: str) -> ZarrDataSource:
        """Return the long-lived ``ZarrDataSource`` for ``store_url`` (opens if needed)."""
        return self._ensure_open(store_url)

    def time_index(self, store_url: str) -> dict[pd.Timestamp, tuple[object, str]]:
        """Return the {timestamp: (raw_timestamp, iso_string)} map for
        ``store_url`` (or empty dict)."""
        with self._lock:
            return self._time_index.get(store_url, {})

    def resolve_timestamp(self, store_url: str, ts: pd.Timestamp) -> object | None:
        """Resolve an already-parsed UTC timestamp to the store's raw
        timestamp value, or None if no such instant exists.
        """
        entry = self.time_index(store_url).get(ts)
        return entry[0] if entry is not None else None

    def is_available(self, store_url: str) -> bool:
        """True unless the last prewarm of ``store_url`` recorded a failure."""
        with self._lock:
            return store_url not in self._failed_stores

    def _mark_failed(self, store_url: str, error: BaseException) -> None:
        with self._lock:
            self._failed_stores[store_url] = error

    def _mark_healthy(self, store_url: str) -> None:
        with self._lock:
            self._failed_stores.pop(store_url, None)

    async def _prewarm_one(self, store_url: str) -> BaseException | None:
        """Open one URL and confirm it can serve tiler requests. None on
        success, else the exception — also recorded via ``_mark_failed`` so
        ``is_available`` reflects it immediately.

        Not-a-grid, not-there, and no-time-dimension are confirmed and not
        retried; anything else gets bounded retries with backoff.
        """
        last_error: BaseException | None = None
        for attempt in range(1, _PREWARM_MAX_ATTEMPTS + 1):
            try:
                await anyio.to_thread.run_sync(
                    self._ensure_open, store_url, limiter=_STORE_PREWARM_LIMITER
                )
                self._mark_healthy(store_url)
                return None
            except NotGriddedStoreError as e:
                logger.info(f"Store is not a lat/lon grid, skipping: {store_url} ({e})")
                self._mark_failed(store_url, e)
                return e
            except NoTimeDimensionError as e:
                logger.info(f"Store has no time dimension, skipping: {store_url} ({e})")
                self._mark_failed(store_url, e)
                return e
            except FileNotFoundError as e:
                # Usually an upstream rename the catalogue hasn't caught up with.
                logger.warning(f"Store does not exist: {store_url} ({e})")
                self._mark_failed(store_url, e)
                return e
            except Exception as e:
                last_error = e
                if attempt < _PREWARM_MAX_ATTEMPTS:
                    delay = _PREWARM_BACKOFF_SECONDS * 2 ** (attempt - 1)
                    logger.warning(
                        f"Store open failed (attempt {attempt}/{_PREWARM_MAX_ATTEMPTS}), "
                        f"retrying in {delay:.1f}s: {store_url} ({e!r})"
                    )
                    await anyio.sleep(delay)
                else:
                    logger.error(
                        f"Store open failed after {_PREWARM_MAX_ATTEMPTS} attempts: "
                        f"{store_url}",
                        exc_info=e,
                    )
        self._mark_failed(store_url, last_error)
        return last_error

    async def prewarm(self, store_urls: list[str]) -> dict[str, BaseException | None]:
        """Open every URL in parallel and report the per-URL outcome.

        Moves the one-time S3 metadata cost from the first user request to server
        startup, and lets get_products_availability respond fast on first call.

        Returns ``{url: None on success, else the exception}``.
        """
        outcomes: dict[str, BaseException | None] = {}
        logger.debug("Prewarming %d stores: %s", len(store_urls), store_urls)

        async def _one(url: str) -> None:
            outcomes[url] = await self._prewarm_one(url)

        await asyncio.gather(*(_one(url) for url in store_urls))

        not_gridded = sum(
            1 for e in outcomes.values() if isinstance(e, NotGriddedStoreError)
        )
        absent = sum(1 for e in outcomes.values() if isinstance(e, FileNotFoundError))
        no_time = sum(
            1 for e in outcomes.values() if isinstance(e, NoTimeDimensionError)
        )
        unresolved = sum(
            1
            for e in outcomes.values()
            if e is not None
            and not isinstance(
                e, (NotGriddedStoreError, FileNotFoundError, NoTimeDimensionError)
            )
        )
        logger.info(
            "Store prewarm complete: %d opened, %d not gridded, %d absent, "
            "%d no time dimension, %d unresolved (of %d)",
            len(outcomes) - not_gridded - absent - no_time - unresolved,
            not_gridded,
            absent,
            no_time,
            unresolved,
            len(outcomes),
        )
        return outcomes

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._stores.clear()
            self._opened_at.clear()
            self._ttl_jitter.clear()
            self._refreshing.clear()
            self._in_flight.clear()
            self._time_index.clear()
            self._failed_stores.clear()

    def _publish(
        self,
        store_url: str,
        source: ZarrDataSource,
        index: dict[pd.Timestamp, tuple[object, str]],
    ) -> None:
        """Atomically replace source, opened-at timestamp, and time index for a URL."""
        with self._lock:
            self._stores[store_url] = source
            self._opened_at[store_url] = time.monotonic()
            self._ttl_jitter[store_url] = random.uniform(
                0.0, self._ttl * _REFRESH_JITTER_FRACTION
            )
            self._time_index[store_url] = index

    def _refresh_background(self, store_url: str) -> None:
        try:
            source = _open_store(store_url)
            index = _build_time_index(source.zarr_store)
            self._publish(store_url, source, index)
            logger.info(f"Store refreshed: {store_url}")
        except Exception:
            logger.exception(f"Background refresh failed: {store_url}")
        finally:
            with self._lock:
                self._refreshing.discard(store_url)


store_registry = StoreRegistry(_STORE_TTL)


def get_store(store_url: str) -> xr.Dataset:
    return store_registry.get(store_url)


def get_datasource(store_url: str) -> ZarrDataSource:
    return store_registry.get_datasource(store_url)


def is_store_available(store_url: str) -> bool:
    return store_registry.is_available(store_url)


def get_available_dates(store_url: str) -> list[tuple[str, pd.Timestamp]]:
    """Return [(iso_string, timestamp)] sorted by timestamp, for `store_url`."""
    get_store(store_url)  # ensures the time index for this URL is populated
    index = store_registry.time_index(store_url)
    return [(iso, ts) for ts, (_raw, iso) in index.items()]


def resolve_timestamp(store_url: str, ts: pd.Timestamp) -> object | None:
    get_store(store_url)  # ensures the time index for this URL is populated
    return store_registry.resolve_timestamp(store_url, ts)


def unavailable_date_message(store_url: str, ts: pd.Timestamp) -> str:
    """ "No data for date ..." message, with a latest-available-date hint.

    Shared by the route-level fail-fast guard (``shared.resolve_timestamp_or_404``)
    and the deep check in ``slice_loader._fetch_slice_from_store``, so the two
    call sites can't drift apart.
    """
    index = store_registry.time_index(store_url)
    latest = index[max(index)][1] if index else None
    hint = (
        f" Latest available date is {latest!r}."
        if latest
        else " No dates are available."
    )
    return f"No data for date {ts_to_utc_iso(ts)!r}.{hint}"


async def prewarm_stores(store_urls: list[str]) -> dict[str, BaseException | None]:
    """Prewarm every URL and return the per-URL outcome map."""
    return await store_registry.prewarm(store_urls)
