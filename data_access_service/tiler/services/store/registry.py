"""Per-URL registry of tiler parquet metadata sidecars.

No zarr access here — the live tiler API never opens a zarr store. All
zarr-touching work (product discovery, store prewarm against real zarr, the
zarr -> parquet conversion itself) lives in ``batch.tiler``; this registry
only reads what that job publishes: each store's ``metadata.json`` sidecar
(lat/lon/shape/timestamps/variable attrs), read from
``TilerParquetConfig.output_dir`` (local disk for now; S3 once the batch job
uploads there — see ``batch.tiler.generator``).

``get_store`` returns a coords-only ``xr.Dataset`` (time/lat/lon, no data
variables) built from the sidecar, so callers that only need the store's
shape/coordinate range (``product.py::get_lod_grids``, ``masks.py``,
``data_tiles.py``) keep working unchanged. Actual pixel values come from
``slice_loader``, which queries the parquet files directly via duckdb.
"""

from __future__ import annotations

import json
import logging
import os
import threading

import pandas as pd
import xarray as xr

from data_access_service.config.config import Config
from data_access_service.models.tiler_parquet_types import TilerParquetMetadata
from data_access_service.tiler.utils.dates import ts_to_utc_iso

logger = logging.getLogger(__name__)


def _dataset_stem(store_url: str) -> str:
    """``s3://.../foo.zarr`` -> ``foo``, matching batch.tiler.parquet_generator's own key."""
    return store_url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".zarr")


def _metadata_path(store_url: str) -> str:
    output_dir = Config.get_config().get_tiler_parquet_config().output_dir
    return os.path.join(output_dir, _dataset_stem(store_url), "metadata.json")


def _load_metadata(store_url: str) -> TilerParquetMetadata:
    with open(_metadata_path(store_url)) as f:
        return TilerParquetMetadata.from_dict(json.load(f))


def _build_time_index(meta: TilerParquetMetadata) -> dict[pd.Timestamp, str]:
    """``{timestamp: raw_timestamp_string}``. The sidecar's own timestamp
    strings are already the store's native representation (see
    ``batch.tiler.parquet_generator._ts_native``) and are what the parquet's
    own ``timestamp`` column holds — the "Z" is stripped only to build the
    lookup key, since every parsed client timestamp is naive-UTC (see
    ``tiler.utils.dates.str_to_utc_timestamp``) and a tz-aware/tz-naive
    ``Timestamp`` comparison raises rather than ever matching.
    """
    return {pd.Timestamp(raw.rstrip("Z")): raw for raw in meta.timestamps}


def _to_dataset(meta: TilerParquetMetadata) -> xr.Dataset:
    """A coords-only Dataset built from the sidecar — no data, just
    time/lat/lon, so ``store.sizes["lat"]``/``["lon"]`` (used by
    ``product.py::get_lod_grids``) and ``store.lat``/``store.lon`` value
    ranges (used by land/ocean masking) keep working unchanged.
    """
    return xr.Dataset(
        coords={
            "time": [pd.Timestamp(raw.rstrip("Z")) for raw in meta.timestamps],
            "lat": meta.lat,
            "lon": meta.lon,
        }
    )


class StoreRegistry:
    """See module docstring. Loads each store's sidecar once, on first
    request, and caches it in-process; ``refresh`` re-reads it.
    """

    def __init__(self) -> None:
        self._metadata: dict[str, TilerParquetMetadata] = {}
        self._datasets: dict[str, xr.Dataset] = {}
        self._time_index: dict[str, dict[pd.Timestamp, str]] = {}
        self._failed_stores: dict[str, BaseException] = {}
        self._lock = threading.Lock()

    def _publish(self, store_url: str, meta: TilerParquetMetadata) -> None:
        index = _build_time_index(meta)
        ds = _to_dataset(meta)
        with self._lock:
            self._metadata[store_url] = meta
            self._datasets[store_url] = ds
            self._time_index[store_url] = index
            self._failed_stores.pop(store_url, None)

    def _ensure_loaded(self, store_url: str) -> TilerParquetMetadata:
        with self._lock:
            meta = self._metadata.get(store_url)
        if meta is not None:
            return meta
        try:
            meta = _load_metadata(store_url)
        except Exception as e:
            with self._lock:
                self._failed_stores[store_url] = e
            raise
        self._publish(store_url, meta)
        return meta

    def get_metadata(self, store_url: str) -> TilerParquetMetadata:
        return self._ensure_loaded(store_url)

    def get(self, store_url: str) -> xr.Dataset:
        """Return the coords-only (time/lat/lon) view, loading the sidecar if needed."""
        self._ensure_loaded(store_url)
        with self._lock:
            return self._datasets[store_url]

    def time_index(self, store_url: str) -> dict[pd.Timestamp, str]:
        with self._lock:
            return self._time_index.get(store_url, {})

    def resolve_timestamp(self, store_url: str, ts: pd.Timestamp) -> str | None:
        """Resolve an already-parsed UTC timestamp to the store's native
        timestamp string (the parquet's own ``timestamp`` column value), or
        None if no such instant exists.
        """
        return self.time_index(store_url).get(ts)

    def is_available(self, store_url: str) -> bool:
        """True unless the last prewarm of ``store_url`` recorded a failure."""
        with self._lock:
            return store_url not in self._failed_stores

    def prewarm(self, store_urls: list[str]) -> dict[str, BaseException | None]:
        """Load (or confirm already-loaded) every URL's sidecar.

        Returns ``{url: None on success, else the exception}`` — the same
        contract ``batch.tiler.zarr_registry.prewarm_stores`` uses for the
        real zarr open, so callers written against either behave the same.
        """
        outcomes: dict[str, BaseException | None] = {}
        for url in store_urls:
            try:
                self._ensure_loaded(url)
                outcomes[url] = None
            except Exception as e:
                logger.warning("Metadata sidecar unavailable for %s: %s", url, e)
                outcomes[url] = e
        opened = sum(1 for outcome in outcomes.values() if outcome is None)
        logger.info(
            "Store prewarm complete: %d opened, %d failed (of %d)",
            opened,
            len(outcomes) - opened,
            len(outcomes),
        )
        return outcomes

    def refresh(self, store_urls: list[str] | None = None) -> None:
        """Re-read the sidecar for every currently-loaded store (or
        ``store_urls`` if given), one at a time. One store's failure is
        logged and does not stop the sweep.
        """
        with self._lock:
            urls = (
                list(self._metadata.keys()) if store_urls is None else store_urls
            )
        for store_url in urls:
            try:
                meta = _load_metadata(store_url)
                self._publish(store_url, meta)
                logger.info(f"Store metadata refreshed: {store_url}")
            except Exception:
                logger.exception(f"Store metadata refresh failed: {store_url}")

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._metadata.clear()
            self._datasets.clear()
            self._time_index.clear()
            self._failed_stores.clear()


store_registry = StoreRegistry()


def get_store(store_url: str) -> xr.Dataset:
    return store_registry.get(store_url)


def get_store_metadata(store_url: str) -> TilerParquetMetadata:
    return store_registry.get_metadata(store_url)


def is_store_available(store_url: str) -> bool:
    return store_registry.is_available(store_url)


def get_available_dates(store_url: str) -> list[tuple[str, pd.Timestamp]]:
    """Return [(iso_string, timestamp)] sorted by timestamp, for `store_url`."""
    get_store_metadata(store_url)  # ensures the time index for this URL is populated
    index = store_registry.time_index(store_url)
    return [(ts_to_utc_iso(ts), ts) for ts in index]


def resolve_timestamp(store_url: str, ts: pd.Timestamp) -> str | None:
    get_store_metadata(store_url)  # ensures the time index for this URL is populated
    return store_registry.resolve_timestamp(store_url, ts)


def unavailable_date_message(store_url: str, ts: pd.Timestamp) -> str:
    """ "No data for date ..." message, with a latest-available-date hint.

    Shared by the route-level fail-fast guard (``shared.resolve_timestamp_or_404``)
    and the deep check in ``slice_loader._fetch_slice_from_store``, so the two
    call sites can't drift apart.
    """
    index = store_registry.time_index(store_url)
    latest = ts_to_utc_iso(max(index)) if index else None
    hint = (
        f" Latest available date is {latest!r}."
        if latest
        else " No dates are available."
    )
    return f"No data for date {ts_to_utc_iso(ts)!r}.{hint}"


async def prewarm_stores(store_urls: list[str]) -> dict[str, BaseException | None]:
    """Prewarm every URL and return the per-URL outcome map.

    Async for call-site compatibility with the FastAPI startup path (``await
    prewarm_stores(...)``) — reading local metadata.json sidecars is fast
    enough that no real concurrency/threading is needed here, unlike the
    batch job's zarr prewarm.
    """
    return store_registry.prewarm(store_urls)


def refresh_stores() -> None:
    """Re-read every currently-loaded store's metadata.json (the periodic cron sweep)."""
    store_registry.refresh()
