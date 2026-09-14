"""Slice loading.

``load_slice`` returns a fully-computed 2-D slice for a (store, timestamp,
variables) tuple. Concurrent identical requests always share one compute
in-process via ``_slice_dedup`` (independent of ``CACHE_BACKEND``); when
``CACHE_BACKEND=redis``, ``slice_memo`` additionally coalesces across
instances and caches the result (see ``services.caching.slice_cache``).

Callers pass an already-parsed ``pd.Timestamp`` (from
``core.tiler_routes.shared.parse_date_or_422``), not a raw date string —
every route handler parses/validates its ``date`` query param exactly once,
so this module never re-parses a string it was already handed as a
``pd.Timestamp``.

Long-lived store handles live in their own module ([[store.registry]]).
Time selection goes through ``aodn_cloud_optimised`` ``ZarrDataSource.get_data``.
"""

import logging

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

from data_access_service.tiler.services.caching.deduper import Deduper
from data_access_service.tiler.services.caching.slice_cache import slice_memo
from data_access_service.tiler.services.rendering.masks import apply_ocean_mask
from data_access_service.tiler.services.store.registry import (
    get_datasource,
    get_store,
    resolve_timestamp,
    unavailable_date_message,
)
from data_access_service.tiler.services.store.spatial import (
    ReadBBox,
    round_bbox,
    snap_read_bbox,
)
from data_access_service.tiler.utils.dates import ts_to_utc_iso

# Always in-process, independent of CACHE_BACKEND — see Deduper's docstring
# for why this matters even (especially) under CACHE_BACKEND=none.
_slice_dedup = Deduper()


def _ts_for_get_data(ts) -> str:
    """Format a timestamp for ``ZarrDataSource.get_data`` date bounds."""
    return pd.Timestamp(ts).isoformat()


def _warm_coord_indexes(ds: xr.Dataset) -> xr.Dataset:
    """Force-build the lazy pandas index engine for lat/lon, once, here."""
    for dim in ("lon", "lat"):
        if dim in ds.indexes:
            ds.indexes[dim].is_unique
    return ds


def _compute_slice_from_store(
    store_url: str,
    ts: pd.Timestamp,
    variables: list[str],
    ocean_masked: bool = False,
    bbox: ReadBBox | None = None,
) -> xr.Dataset:
    """Fetch a 2-D slice from the Zarr store. Both `load_slice` and
    `load_slice_uncached` delegate here; they differ only in whether the
    result lands in L1.

    When ``ocean_masked`` is set, anomalous values outside the model's valid ocean
    domain are nulled here (masks.apply_ocean_mask) so every downstream consumer
    inherits the cut.

    ``bbox`` is an optional store-frame (lon_min, lat_min, lon_max, lat_max)
    window passed through to ``get_data`` so Zarr only decompresses overlapping
    chunks. ``None`` means the full lat×lon frame (data tiles, point lookup).
    """
    result = _fetch_slice_from_store(store_url, ts, variables, bbox=bbox)
    if ocean_masked:
        result = apply_ocean_mask(result, variables)
    return result


def _fetch_slice_from_store(
    store_url: str,
    ts: pd.Timestamp,
    variables: list[str],
    bbox: ReadBBox | None = None,
) -> xr.Dataset:
    # Ensure store is open (time index + variable catalogue on normalised view).
    store = get_store(store_url)

    missing = [v for v in variables if v not in store.data_vars]
    if missing:
        raise FileNotFoundError(
            f"Variable(s) {missing} not found in store {store_url!r} "
            f"(available: {sorted(store.data_vars)})"
        )

    t0 = resolve_timestamp(store_url, ts)
    if t0 is None:
        raise FileNotFoundError(unavailable_date_message(store_url, ts))

    spatial: dict[str, float] = {}
    if bbox is not None:
        lon_min, lat_min, lon_max, lat_max = bbox
        spatial = {
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max,
        }

    try:
        logger.info(
            "L1 cache miss — fetching Zarr from S3 (not cache): store=%s ts=%s vars=%s bbox=%s",
            store_url,
            ts_to_utc_iso(ts),
            variables,
            bbox,
        )
        ds = get_datasource(store_url).get_data(
            date_start=_ts_for_get_data(t0),
            date_end=_ts_for_get_data(t0),
            **spatial,
        )
        ds = ds[variables]
        # get_data returns a time range (often length 1). Match previous
        # .sel(time=scalar) behaviour: one frame, time dim dropped.
        if "time" in ds.dims:
            if ds.sizes["time"] == 0:
                raise KeyError(ts)
            ds = ds.isel(time=0)
        return ds.compute() if hasattr(ds, "compute") else ds
    except KeyError as e:
        raise FileNotFoundError(f"No data found for date {ts_to_utc_iso(ts)}") from e


def _cache_key(
    store_url: str,
    ts: pd.Timestamp,
    variables: list[str],
    bbox: ReadBBox | None,
) -> tuple:
    spatial = round_bbox(bbox) if bbox is not None else None
    return (store_url, ts, tuple(sorted(variables)), spatial)


_NO_OVERLAP = object()


def _resolve_read_bbox(
    store_url: str, bbox: ReadBBox | None, pad_cells: int
) -> ReadBBox | None | object:
    """Snap a WGS84 window to store chunks. ``None`` input = full frame.

    Returns ``_NO_OVERLAP`` when the window does not intersect the store.
    """
    if bbox is None:
        return None
    snapped = snap_read_bbox(store_url, bbox, pad_cells=pad_cells)
    if snapped is None:
        return _NO_OVERLAP
    return snapped


def _empty_frame(store_url: str, variables: list[str]) -> xr.Dataset:
    store = get_store(store_url)
    ds = store[variables]
    if "time" in ds.dims:
        ds = ds.isel(time=0, drop=True)
    return ds.isel(lat=slice(0, 0), lon=slice(0, 0))


def load_slice(
    store_url: str,
    ts: pd.Timestamp,
    variables: list[str],
    ocean_masked: bool = False,
    bbox: ReadBBox | None = None,
    pad_cells: int = 2,
) -> xr.Dataset:
    """
    Return a fully-computed 2D (lat × lon) slice for the given store, timestamp,
    and variables. ``ts`` must name an exact instant in the store's time index —
    no nearest-match fallback. Coordinate names are normalised to
    ``time``/``lat``/``lon`` before return.

    ``bbox`` is a WGS84 (lon_min, lat_min, lon_max, lat_max) window (visual
    tiles). It is snapped to overlapping Zarr chunks before the fetch, and is
    part of the L1 key, so two windows do not share a cached full frame.
    ``None`` means the full lat×lon frame (data tiles, point lookup).
    ``ocean_masked`` (from ``Product.ocean_masked``) nulls anomalous values outside
    the valid model domain. It's a deterministic function of the cache key (a store
    + variable set maps to one product), so it stays out of the key; the masked
    slice is what L1 caches.
    """
    read_bbox = _resolve_read_bbox(store_url, bbox, pad_cells)
    if read_bbox is _NO_OVERLAP:
        return _empty_frame(store_url, variables)

    cache_key = _cache_key(store_url, ts, variables, read_bbox)

    def compute() -> xr.Dataset:
        result = slice_memo.get_or_compute(
            cache_key,
            lambda: _compute_slice_from_store(
                store_url, ts, variables, ocean_masked, bbox=read_bbox
            ),
        )

        return _warm_coord_indexes(result)

    return _slice_dedup.dedupe(cache_key, compute)


def load_slice_uncached(
    store_url: str,
    ts: pd.Timestamp,
    variables: list[str],
    ocean_masked: bool = False,
    bbox: ReadBBox | None = None,
    pad_cells: int = 2,
) -> xr.Dataset:
    """Return a 2-D slice without touching L1.

    Pulls via lib ``get_data``. Used by the animation endpoint so a
    rare multi-date request doesn't evict another product's hot slices from
    the shared L1 cache (CACHE_BACKEND=redis).
    """
    read_bbox = _resolve_read_bbox(store_url, bbox, pad_cells)
    if read_bbox is _NO_OVERLAP:
        return _empty_frame(store_url, variables)
    return _compute_slice_from_store(
        store_url, ts, variables, ocean_masked, bbox=read_bbox
    )
