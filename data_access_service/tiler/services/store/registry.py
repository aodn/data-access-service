"""Per-URL registry of tiler parquet metadata sidecars.

No zarr access here — all zarr-touching work lives in ``batch.tiler``. This
registry only reads what that job publishes: each store's ``metadata.json``
sidecar, from S3 at ``TilerParquetConfig.output_dir``.

``get_store`` returns a coords-only ``xr.Dataset`` (time/lat/lon, no data)
built from the sidecar. Actual pixel values come from ``slice_loader``,
which reads the parquet files directly via duckdb.
"""

from __future__ import annotations

import logging
import threading

import pandas as pd
import xarray as xr

from data_access_service.config.config import Config
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    dataset_stem,
)
from data_access_service.tiler.utils.dates import ts_to_utc_iso
from data_access_service.tiler.utils.s3_json import read_json

logger = logging.getLogger(__name__)


def _metadata_path(store_url: str) -> str:
    output_dir = Config.get_config().get_tiler_parquet_config().output_dir
    return f"{output_dir.rstrip('/')}/{dataset_stem(store_url)}/metadata.json"


def _load_metadata(store_url: str) -> TilerParquetMetadata:
    return TilerParquetMetadata.from_dict(read_json(_metadata_path(store_url)))


def _build_time_index(meta: TilerParquetMetadata) -> dict[pd.Timestamp, str]:
    """``{timestamp: raw_timestamp_string}``. "Z" is stripped only for the
    lookup key — parsed client timestamps are naive-UTC, and a tz-aware key
    would never match.
    """
    return {pd.Timestamp(raw.rstrip("Z")): raw for raw in meta.timestamps}


def _to_dataset(meta: TilerParquetMetadata, times: list[pd.Timestamp]) -> xr.Dataset:
    """A coords-only Dataset (time/lat/lon, no data) built from the sidecar."""
    return xr.Dataset(coords={"time": times, "lat": meta.lat, "lon": meta.lon})


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
        ds = _to_dataset(meta, list(index))
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
        """Load every URL's sidecar. Returns ``{url: None on success, else
        the exception}``.
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

    def refresh(self) -> None:
        """Re-read the sidecar for every currently-loaded store, one at a
        time. One store's failure is logged and does not stop the sweep.
        """
        with self._lock:
            urls = list(self._metadata.keys())
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
    """ "No data for date ..." message, with a latest-available-date hint."""
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

    Async only so the FastAPI startup path can ``await`` it — the actual
    work is a fast synchronous JSON read.
    """
    return store_registry.prewarm(store_urls)


def refresh_stores() -> None:
    """Re-read every currently-loaded store's metadata.json (the periodic cron sweep)."""
    store_registry.refresh()
