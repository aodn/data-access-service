"""Per-store registry of tiler parquet metadata sidecars.

No zarr access here — all zarr-touching work lives in ``batch.tiler``. This
registry only reads what that job publishes: each store's ``metadata.json``
sidecar, from S3 at ``TilerParquetConfig.output_dir``.

``get_store_metadata`` returns the sidecar itself (grid, timestamps,
per-variable dtype/attrs). Actual pixel values come from ``slice_loader``,
which reads the parquet files directly via duckdb.
"""

from __future__ import annotations

import logging
import threading

import pandas as pd

from data_access_service.config.config import Config
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    store_metadata_path,
)
from data_access_service.tiler.utils.dates import ts_to_utc_iso
from data_access_service.tiler.utils.s3_json import read_json

logger = logging.getLogger(__name__)


def _metadata_path(store: str) -> str:
    output_dir = Config.get_config().get_tiler_parquet_config().output_dir
    return store_metadata_path(output_dir, store)


def _load_metadata(store: str) -> TilerParquetMetadata:
    return TilerParquetMetadata.from_dict(read_json(_metadata_path(store)))


def _build_time_index(meta: TilerParquetMetadata) -> dict[pd.Timestamp, str]:
    """``{timestamp: raw_timestamp_string}``. "Z" is stripped only for the
    lookup key — parsed client timestamps are naive-UTC, and a tz-aware key
    would never match.
    """
    return {pd.Timestamp(raw.rstrip("Z")): raw for raw in meta.timestamps}


class StoreRegistry:
    """See module docstring. Loads each store's sidecar once, on first
    request, and caches it in-process; ``refresh`` re-reads it.
    """

    def __init__(self) -> None:
        self._metadata: dict[str, TilerParquetMetadata] = {}
        self._time_index: dict[str, dict[pd.Timestamp, str]] = {}
        self._failed_stores: dict[str, BaseException] = {}
        self._lock = threading.Lock()

    def _publish(self, store: str, meta: TilerParquetMetadata) -> None:
        index = _build_time_index(meta)
        with self._lock:
            self._metadata[store] = meta
            self._time_index[store] = index
            self._failed_stores.pop(store, None)

    def _ensure_loaded(self, store: str) -> TilerParquetMetadata:
        with self._lock:
            meta = self._metadata.get(store)
        if meta is not None:
            return meta
        try:
            meta = _load_metadata(store)
        except Exception as e:
            with self._lock:
                self._failed_stores[store] = e
            raise
        self._publish(store, meta)
        return meta

    def get_metadata(self, store: str) -> TilerParquetMetadata:
        return self._ensure_loaded(store)

    def time_index(self, store: str) -> dict[pd.Timestamp, str]:
        with self._lock:
            return self._time_index.get(store, {})

    def resolve_timestamp(self, store: str, ts: pd.Timestamp) -> str | None:
        """Resolve an already-parsed UTC timestamp to the store's native
        timestamp string (the parquet's own ``timestamp`` column value), or
        None if no such instant exists.
        """
        return self.time_index(store).get(ts)

    def is_available(self, store: str) -> bool:
        """True unless the last prewarm of ``store`` recorded a failure."""
        with self._lock:
            return store not in self._failed_stores

    def prewarm(self, stores: list[str]) -> dict[str, BaseException | None]:
        """Load every store's sidecar. Returns ``{store: None on success, else
        the exception}``.
        """
        outcomes: dict[str, BaseException | None] = {}
        for store in stores:
            try:
                self._ensure_loaded(store)
                outcomes[store] = None
            except Exception as e:
                logger.warning("Metadata sidecar unavailable for %s: %s", store, e)
                outcomes[store] = e
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
            stores = list(self._metadata.keys())
        for store in stores:
            try:
                meta = _load_metadata(store)
                self._publish(store, meta)
                logger.info(f"Store metadata refreshed: {store}")
            except Exception:
                logger.exception(f"Store metadata refresh failed: {store}")

    def clear(self) -> None:
        """Drop all cached state. Intended for tests."""
        with self._lock:
            self._metadata.clear()
            self._time_index.clear()
            self._failed_stores.clear()


store_registry = StoreRegistry()


def get_store_metadata(store: str) -> TilerParquetMetadata:
    return store_registry.get_metadata(store)


def is_store_available(store: str) -> bool:
    return store_registry.is_available(store)


def get_available_dates(store: str) -> list[tuple[str, pd.Timestamp]]:
    """Return [(iso_string, timestamp)] sorted by timestamp, for `store`."""
    get_store_metadata(store)  # ensures the time index for this store is populated
    index = store_registry.time_index(store)
    return [(ts_to_utc_iso(ts), ts) for ts in index]


def resolve_timestamp(store: str, ts: pd.Timestamp) -> str | None:
    get_store_metadata(store)  # ensures the time index for this store is populated
    return store_registry.resolve_timestamp(store, ts)


def unavailable_date_message(store: str, ts: pd.Timestamp) -> str:
    """ "No data for date ..." message, with a latest-available-date hint."""
    index = store_registry.time_index(store)
    latest = ts_to_utc_iso(max(index)) if index else None
    hint = (
        f" Latest available date is {latest!r}."
        if latest
        else " No dates are available."
    )
    return f"No data for date {ts_to_utc_iso(ts)!r}.{hint}"


async def prewarm_stores(stores: list[str]) -> dict[str, BaseException | None]:
    """Prewarm every store and return the per-store outcome map.

    Async only so the FastAPI startup path can ``await`` it — the actual
    work is a fast synchronous JSON read.
    """
    return store_registry.prewarm(stores)


def refresh_stores() -> None:
    """Re-read every currently-loaded store's metadata.json (the periodic cron sweep)."""
    store_registry.refresh()
