"""Per-URL registry of long-lived xarray.Dataset handles backed by Zarr stores.

Not a strict cache: handles are never evicted, and ``ttl`` triggers a
background refresh rather than expiry, so requests never block on freshness
after the first open.

Also builds a per-store ``{local_date: [timestamps]}`` index so
``load_slice`` / ``get_available_dates`` can resolve a local date without
scanning every timestamp.
"""

import asyncio
import concurrent.futures
import logging
import threading
import time

import anyio
import xarray as xr

from data_access_service.config.config import Config
from data_access_service.config.tiler.constants import COORD_NAMES
from data_access_service.tiler.utils.dates import ts_to_local_date

logger = logging.getLogger(__name__)

_tiler_config = Config.get_config().get_tiler_config()

_STORE_TTL = float(_tiler_config.store_ttl_seconds)

# Capacity gate for concurrent store opens during prewarm, bounded to the S3
# connection ceiling rather than CPU. Kept as its own budget so a many-product
# startup can't consume all tile-handler thread pool slots.
_STORE_PREWARM_LIMITER = anyio.CapacityLimiter(_tiler_config.store_prewarm_workers)

# Per-syscall timeouts on every S3 connection, so a stuck socket can't pin a
# worker thread indefinitely (Python threads can't be cancelled). Passed via
# `config_kwargs`, not `client_kwargs`: s3fs already uses a `config` key
# internally and a collision there raises TypeError.
_S3_CONFIG_KWARGS = {
    "connect_timeout": _tiler_config.s3_connect_timeout,
    "read_timeout": _tiler_config.s3_read_timeout,
    "retries": {"max_attempts": _tiler_config.s3_max_attempts, "mode": "standard"},
}


def _storage_options(store_url: str) -> dict:
    """Storage-backend options for fsspec/zarr, derived from the URL scheme.

    ``s3://`` defaults to anonymous access (AODN buckets are public); set
    ``tiler.s3_anon: false`` to let fsspec discover AWS credentials instead,
    for private buckets. Other schemes get no options — fsspec picks defaults.
    """
    if store_url.startswith("s3://"):
        return {"anon": _tiler_config.s3_anon, "config_kwargs": _S3_CONFIG_KWARGS}
    return {}


# Spatial dims merge into a single dask chunk per variable, covering both
# canonical ("lat"/"lon") and raw pre-rename names ("LATITUDE"/"LONGITUDE"),
# since rename to canonical names happens *after* open. "time" is left unset
# so it keeps the store's native chunk size instead of requiring it upfront.
_SPATIAL_CHUNK_DIMS = {"lat", "lon"} | {
    raw for raw, canonical in COORD_NAMES.items() if canonical in ("lat", "lon")
}


def _open_store(store_url: str) -> xr.Dataset:
    # Reads always select by time over the full lat/lon extent, so native spatial
    # chunking buys no read-efficiency but costs one dask task per native chunk —
    # finely-chunked stores can produce 10M+ chunks, tens of GB just to describe
    # the data before fetching any of it. Merging spatial dims collapses that to
    # one task per time-chunk with no change in bytes fetched or latency (~165x
    # less memory on the worst-case store). Do NOT also merge "time" or switch it
    # to chunks='auto' — either would turn a one-slice read into a multi-slice S3
    # over-read (tests mock open_zarr with numpy datasets, so this regression
    # wouldn't show up in CI).
    ds = xr.open_zarr(
        store_url,
        chunks={dim: -1 for dim in _SPATIAL_CHUNK_DIMS},
        storage_options=_storage_options(store_url),
    )
    rename = {k: v for k, v in COORD_NAMES.items() if k in ds.dims or k in ds.coords}
    if rename:
        ds = ds.rename(rename)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise ValueError(
            f"Store {store_url!r} missing lat/lon dims after rename (found: {list(ds.dims)})"
        )
    if "time" in ds.dims:
        ds = ds.sortby("time")
    return ds


def _build_date_index(ds: xr.Dataset) -> dict[str, list]:
    """Return {local_date: [timestamps]} for the dataset's time coord, or {} if missing."""
    if "time" not in ds.dims:
        return {}
    index: dict[str, list] = {}
    for ts in ds.coords["time"].values:
        index.setdefault(ts_to_local_date(ts), []).append(ts)
    return index


class StoreRegistry:
    """See module docstring for the design.

    Concurrent first-time opens of the *same* URL share one ``xr.open_zarr``
    call via a per-URL ``Future``; opens of *different* URLs run in parallel.
    """

    def __init__(self, ttl: float) -> None:
        self._ttl = ttl
        self._stores: dict[str, xr.Dataset] = {}
        self._opened_at: dict[str, float] = {}
        self._refreshing: set[str] = set()
        self._in_flight: dict[str, concurrent.futures.Future] = {}
        self._date_index: dict[str, dict[str, list]] = {}
        self._lock = threading.Lock()

    def get(self, store_url: str) -> xr.Dataset:
        """Return the dataset for ``store_url``, opening it on first request."""
        should_open = False
        with self._lock:
            if store_url in self._stores:
                if time.monotonic() - self._opened_at[store_url] < self._ttl:
                    return self._stores[store_url]
                # TTL expired — return stale store and trigger a background refresh.
                if store_url not in self._refreshing:
                    self._refreshing.add(store_url)
                    logger.info(
                        f"Store TTL expired, refreshing in background: {store_url}"
                    )
                    threading.Thread(
                        target=self._refresh_background, args=(store_url,), daemon=True
                    ).start()
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
            ds = _open_store(store_url)
            index = _build_date_index(ds)
            self._publish(store_url, ds, index)
            logger.info(f"Store opened: {store_url} (date_count={len(index)})")
            future.set_result(ds)
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            with self._lock:
                self._in_flight.pop(store_url, None)
        return ds

    def date_index(self, store_url: str) -> dict[str, list]:
        """Return the {local_date: [timestamps]} map for ``store_url`` (or empty dict)."""
        with self._lock:
            return self._date_index.get(store_url, {})

    def is_open(self, store_url: str) -> bool:
        """True if ``store_url`` has a successfully-opened handle cached."""
        with self._lock:
            return store_url in self._stores

    async def prewarm(self, store_urls: list[str]) -> None:
        """Open every URL in parallel via the anyio thread pool.

        Moves the one-time S3 metadata cost from the first user request to server
        startup, and lets get_products_availability respond fast on first call.
        Per-URL failures are logged and swallowed so a single bad URL doesn't
        block the others.
        """

        async def _one(url: str) -> None:
            try:
                await anyio.to_thread.run_sync(
                    self.get, url, limiter=_STORE_PREWARM_LIMITER
                )
            except Exception:
                logger.exception(f"Store prewarm failed: {url}")

        await asyncio.gather(*(_one(url) for url in store_urls))

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._stores.clear()
            self._opened_at.clear()
            self._refreshing.clear()
            self._in_flight.clear()
            self._date_index.clear()

    def _publish(self, store_url: str, ds: xr.Dataset, index: dict[str, list]) -> None:
        """Atomically replace store, opened-at timestamp, and date index for a URL."""
        with self._lock:
            self._stores[store_url] = ds
            self._opened_at[store_url] = time.monotonic()
            self._date_index[store_url] = index

    # TODO: periodically refresh all stores from products via a background cron
    # job instead of the current request-triggered refresh, so a product that
    # failed at warmup can still come online later.
    def _refresh_background(self, store_url: str) -> None:
        try:
            ds = _open_store(store_url)
            index = _build_date_index(ds)
            self._publish(store_url, ds, index)
            logger.info(f"Store refreshed: {store_url}")
        except Exception:
            logger.exception(f"Background refresh failed: {store_url}")
        finally:
            with self._lock:
                self._refreshing.discard(store_url)


store_registry = StoreRegistry(_STORE_TTL)


def get_store(store_url: str) -> xr.Dataset:
    return store_registry.get(store_url)


def get_available_dates(store_url: str) -> list[str]:
    index = store_registry.date_index(store_url)
    return sorted(index) if index else []


def is_store_available(store_url: str) -> bool:
    return store_registry.is_open(store_url)


async def prewarm_stores(store_urls: list[str]) -> None:
    try:
        await store_registry.prewarm(store_urls)
    except Exception:
        logger.exception("Store prewarm task exited with error")
