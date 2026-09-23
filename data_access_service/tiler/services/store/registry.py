"""Each store's ``metadata.json`` (grid, timestamps, variable attrs),
loaded from S3 and cached."""

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
    tiler_root_dir = Config.get_config().get_tiler_root_dir()
    return store_metadata_path(tiler_root_dir, store)


def _load_metadata(store: str) -> TilerParquetMetadata:
    return TilerParquetMetadata.from_dict(read_json(_metadata_path(store)))


def _build_time_index(meta: TilerParquetMetadata) -> dict[pd.Timestamp, str]:
    """``{naive-UTC timestamp: raw timestamp string}``."""
    return {pd.Timestamp(raw.rstrip("Z")): raw for raw in meta.timestamps}


# TODO: simplify it.
class StoreRegistry:
    """Store metadata, loaded on first use; ``refresh`` re-reads it."""

    def __init__(self) -> None:
        self._metadata: dict[str, TilerParquetMetadata] = {}
        self._time_index: dict[str, dict[pd.Timestamp, str]] = {}
        # We might could remove _failed_stores, as now it only reads the metadata of each store. self._metadata has all the stores knowledge.
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
        """The store's raw timestamp string for ``ts``, or None."""
        return self.time_index(store).get(ts)

    def is_available(self, store: str) -> bool:
        """False if ``store``'s metadata failed to load."""
        with self._lock:
            return store not in self._failed_stores

    def prewarm(self, stores: list[str]) -> dict[str, BaseException | None]:
        """Load every store's metadata. Returns ``{store: None or the error}``."""
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
        """Re-read every loaded store's metadata; failures are logged."""
        with self._lock:
            stores = list(self._metadata.keys())
        for store in stores:
            try:
                meta = _load_metadata(store)
                self._publish(store, meta)
                logger.info(f"Store metadata refreshed: {store}")
            except Exception:
                logger.exception(f"Store metadata refresh failed: {store}")

    def retain(self, stores: set[str]) -> None:
        """Forget every store not in ``stores``."""
        with self._lock:
            for store in set(self._metadata) | set(self._failed_stores):
                if store not in stores:
                    self._metadata.pop(store, None)
                    self._time_index.pop(store, None)
                    self._failed_stores.pop(store, None)

    def clear(self) -> None:
        """Drop everything (tests)."""
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
    """``[(iso_string, timestamp)]`` for ``store``, sorted."""
    get_store_metadata(store)  # loads the time index
    index = store_registry.time_index(store)
    return [(ts_to_utc_iso(ts), ts) for ts in index]


def resolve_timestamp(store: str, ts: pd.Timestamp) -> str | None:
    get_store_metadata(store)  # loads the time index
    return store_registry.resolve_timestamp(store, ts)


def unavailable_date_message(store: str, ts: pd.Timestamp) -> str:
    """ "No data for date ..." with the latest available date."""
    index = store_registry.time_index(store)
    latest = ts_to_utc_iso(max(index)) if index else None
    hint = (
        f" Latest available date is {latest!r}."
        if latest
        else " No dates are available."
    )
    return f"No data for date {ts_to_utc_iso(ts)!r}.{hint}"


# rename this, it is not really prewarming any more like before, it was reading metadata of zarr and keeping the handle open, now it is just reading the metadata and caching it, so it is more like loading the metadata of stores
def prewarm_stores(stores: list[str]) -> dict[str, BaseException | None]:
    """Load the metadata of each store not loaded yet (failed ones are retried)."""
    return store_registry.prewarm(stores)


def retain_stores(stores: set[str]) -> None:
    """Forget stores that are no longer in the catalogue."""
    store_registry.retain(stores)


def refresh_stores() -> None:
    """Re-read every loaded store's metadata (cron)."""
    store_registry.refresh()
