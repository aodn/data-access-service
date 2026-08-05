"""Per-URL registry of long-lived xarray.Dataset handles backed by Zarr stores.

Not a cache in the strict sense: handles are not evicted (the URL set is small
and bounded by registered products), and ``ttl`` triggers a background refresh
rather than expiry. Stale entries keep serving until the refresh completes, so
requests never block on freshness — only the very first open per URL blocks.

A per-store ``{local_date: [timestamps]}`` index is built alongside the dataset
so ``load_slice`` / ``get_available_dates`` can resolve a local date in O(1)
instead of converting every timestamp on the hot path.
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

# Capacity gate for concurrent store opens during prewarm. Bounded to the S3
# connection ceiling, not CPU. Runs on the shared anyio pool but a separate
# budget so a many-product startup can't transiently consume tile-handler slots.
_STORE_PREWARM_LIMITER = anyio.CapacityLimiter(_tiler_config.store_prewarm_workers)

# Per-syscall timeouts on every S3 connection. Without these, a stuck socket can
# pin a worker thread indefinitely (Python threads can't be cancelled, so a
# request-level wait would free the request but leave the thread held until the
# kernel eventually times out — minutes under bad network conditions).
# Passed via `config_kwargs` (not `client_kwargs`): s3fs builds its own Config
# and passes it as `config=` to create_client, so a `config` key in client_kwargs
# collides with that positional and raises TypeError.
_S3_CONFIG_KWARGS = {
    "connect_timeout": _tiler_config.s3_connect_timeout,
    "read_timeout": _tiler_config.s3_read_timeout,
    "retries": {"max_attempts": _tiler_config.s3_max_attempts, "mode": "standard"},
}


# How hard prewarm tries before giving an operational failure back to its
# caller. Deliberately small: at 60 stores the point is to ride out a transient
# S3 blip, not to stall startup behind a genuinely unreachable bucket. A store
# that is still unresolved after this is handled by the caller's graded policy,
# and StoreRegistry.get does not cache the failure, so it re-attempts on the
# first real request either way.
_PREWARM_MAX_ATTEMPTS = 3
_PREWARM_BACKOFF_SECONDS = 1.0


class NotGriddedStoreError(ValueError):
    """The store opened, but is not a lat/lon grid the tiler can render.

    A *confirmed* answer rather than an operational one: retrying will not
    change it, and every candidate product on the store is dropped rather than
    degraded. Subclasses ValueError because that is what ``_open_store`` has
    always raised and callers catch.
    """


def _storage_options(store_url: str) -> dict:
    """Storage-backend options for fsspec/zarr, derived from the URL scheme.

    - ``s3://`` defaults to anonymous access (IMOS's AODN buckets are public). Set
      ``tiler.s3_anon: false`` in config.yaml to let fsspec discover AWS credentials
      via the standard chain (env vars → ``~/.aws/credentials`` → IAM role) — needed
      for private buckets.
    - Other schemes (``file://``, ``https://``, ``gs://``, plain paths …) pass no
      options; fsspec / its backend picks sensible defaults.
    """
    if store_url.startswith("s3://"):
        return {"anon": _tiler_config.s3_anon, "config_kwargs": _S3_CONFIG_KWARGS}
    return {}


# Spatial dims are merged into a single dask chunk per variable, covering both
# already-canonical names ("lat"/"lon") and the raw names COORD_NAMES renames from
# ("LATITUDE"/"LONGITUDE") — the rename to canonical names happens *after* open, so
# open-time chunk keys must match whatever the store calls them natively. "time" is
# deliberately absent: an unset key falls back to the store's native/preferred chunk
# size (see xarray.structure.chunks._get_chunk), so native time-chunking is kept
# without needing to know each store's native time-chunk size in advance.
_SPATIAL_CHUNK_DIMS = {"lat", "lon"} | {
    raw for raw, canonical in COORD_NAMES.items() if canonical in ("lat", "lon")
}


def _open_store(store_url: str) -> xr.Dataset:
    # slice_loader.py's only read path selects by time and always reads the full
    # lat/lon extent — native spatial chunking buys zero read-efficiency there,
    # since a single time-slice request already touches every native spatial chunk
    # for that time-block. But dask builds one graph task *per native chunk*, and a
    # handful of production stores are chunked finely enough (e.g. [5, 250, 250] on
    # a [8153, 2500, 10000] array) to produce 10M+ chunks for one store alone — tens
    # of GB of real memory just to describe where the data is, before any of it is
    # fetched (confirmed via macOS `footprint`; `ps`/`psutil` RSS does not surface
    # this). Merging spatial dims collapses that to one task per native time-chunk,
    # with no change in bytes fetched per request and no read-latency regression
    # (verified: identical .compute() results, same wall-clock fetch time) — just
    # ~165x less memory to open the worst-case store. Do NOT merge "time" too, and
    # do NOT switch to chunks='auto' for it — either would merge adjacent
    # time-blocks into one dask chunk, turning a one-slice read into a multi-slice
    # S3 over-read. (Tests mock open_zarr with numpy datasets, so such a regression
    # would pass CI but degrade production.)
    ds = xr.open_zarr(
        store_url,
        chunks={dim: -1 for dim in _SPATIAL_CHUNK_DIMS},
        storage_options=_storage_options(store_url),
    )
    rename = {k: v for k, v in COORD_NAMES.items() if k in ds.dims or k in ds.coords}
    if rename:
        ds = ds.rename(rename)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise NotGriddedStoreError(
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

    Concurrent first-time opens of the *same* URL share one ``xr.open_zarr`` call
    via a per-URL ``concurrent.futures.Future``; opens of *different* URLs run in
    parallel (the original implementation serialised them under a single global
    lock until this pattern was introduced).
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

    async def _prewarm_one(self, store_url: str) -> BaseException | None:
        """Open one URL, retrying operational failures. Returns None on success.

        A NotGriddedStoreError is returned immediately: the store answered, and
        the answer will not change on a retry. Anything else is treated as
        operational — a slow bucket, a dropped socket, throttling — and gets a
        bounded number of attempts with exponential backoff before being handed
        back to the caller to grade.
        """
        last_error: BaseException | None = None
        for attempt in range(1, _PREWARM_MAX_ATTEMPTS + 1):
            try:
                await anyio.to_thread.run_sync(
                    self.get, store_url, limiter=_STORE_PREWARM_LIMITER
                )
                return None
            except NotGriddedStoreError as e:
                # Intentional exclusion, not a fault: INFO and no traceback, or
                # 60 stores' worth of legitimate skips look like an incident.
                logger.info(f"Store is not a lat/lon grid, skipping: {store_url} ({e})")
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
        return last_error

    async def prewarm(self, store_urls: list[str]) -> dict[str, BaseException | None]:
        """Open every URL in parallel and report the per-URL outcome.

        Moves the one-time S3 metadata cost from the first user request to server
        startup, and lets get_products_availability respond fast on first call.

        Returns ``{url: None on success, else the exception}`` rather than
        swallowing failures. The caller needs the distinction: a confirmed
        non-grid store drops its products, while an operational failure is graded
        against whether the store backs a product already in production use.
        """
        outcomes: dict[str, BaseException | None] = {}

        async def _one(url: str) -> None:
            outcomes[url] = await self._prewarm_one(url)

        await asyncio.gather(*(_one(url) for url in store_urls))

        not_gridded = sum(
            1 for e in outcomes.values() if isinstance(e, NotGriddedStoreError)
        )
        unresolved = sum(
            1
            for e in outcomes.values()
            if e is not None and not isinstance(e, NotGriddedStoreError)
        )
        logger.info(
            "Store prewarm complete: %d opened, %d not gridded, %d unresolved "
            "(of %d)",
            len(outcomes) - not_gridded - unresolved,
            not_gridded,
            unresolved,
            len(outcomes),
        )
        return outcomes

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
    get_store(store_url)  # ensures the date index for this URL is populated
    index = store_registry.date_index(store_url)
    return sorted(index) if index else []


async def prewarm_stores(store_urls: list[str]) -> dict[str, BaseException | None]:
    """Prewarm every URL and return the per-URL outcome map.

    Deliberately does not catch: startup grades these outcomes and decides what
    is fatal, so swallowing an error here would leave it deciding from an
    incomplete map.
    """
    return await store_registry.prewarm(store_urls)
