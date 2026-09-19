from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import xarray as xr

from data_access_service.batch.tiler import storage
from data_access_service.batch.tiler.zarr_registry import get_datasource, get_store
from data_access_service.core.duckdbclient import TilerBatchDuckDBClient
from data_access_service.models.estimation_types import schema_fingerprint
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    store_metadata_path,
    variable_parquet_path,
)
from data_access_service.models.tiler_types import TilerBatchDuckDBConfig

logger = logging.getLogger(__name__)

VALUE_DTYPE = "float32"

_EXCLUDED_ATTRS = frozenset({"_ChunkSizes"})


def _json_safe(value):
    """CF attrs sometimes carry numpy scalars/arrays; json.dump can't handle those."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _ts_native(ts) -> str:
    return f"{ts}Z"


def _ts_for_get_data(ts) -> str:
    """ISO string for ``ZarrDataSource.get_data``'s date bounds.

    Pure query plumbing, never written anywhere: ``get_data`` matches against
    the store's own naive-UTC time index via pandas, and pandas' own
    ``isoformat()`` rendering is what's proven to parse back correctly there
    (mirrors ``slice_loader._ts_for_get_data``, used for the same reason on
    the live read path).
    """
    return pd.Timestamp(ts).isoformat()


def build_metadata(
    store: str, uuid: str, variables: list[str], timestamps: list[str]
) -> TilerParquetMetadata:
    """Grid + per-variable metadata for ``store``, listing ``timestamps`` as
    converted. Touches only coords/attrs, never the (potentially huge) value
    data.
    """
    ds = get_store(store)  # normalised time/lat/lon view; coords are eager

    missing = [v for v in variables if v not in ds.data_vars]
    if missing:
        raise FileNotFoundError(
            f"Variable(s) {missing} not found in store {store!r} "
            f"(available: {sorted(ds.data_vars)})"
        )

    lat = ds["lat"].values
    lon = ds["lon"].values

    variable_meta = {
        v: TilerVariableMetadata(
            dtype=VALUE_DTYPE,
            attrs={
                k: _json_safe(val)
                for k, val in ds[v].attrs.items()
                if k not in _EXCLUDED_ATTRS
            },
        )
        for v in variables
    }

    return TilerParquetMetadata(
        uuid=uuid,
        dataset=f"{store}.zarr",
        n_i=int(lat.shape[0]),
        n_j=int(lon.shape[0]),
        lat=[float(x) for x in lat],
        lon=[float(x) for x in lon],
        timestamps=timestamps,
        variables=variable_meta,
        schema_fingerprint=schema_fingerprint(variables),
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def read_metadata(output_dir: str, store: str) -> TilerParquetMetadata | None:
    data = storage.read_json(store_metadata_path(output_dir, store))
    return TilerParquetMetadata.from_dict(data) if data is not None else None


def write_metadata(meta: TilerParquetMetadata, output_dir: str, store: str) -> str:
    path = store_metadata_path(output_dir, store)
    storage.write_json(path, meta.to_dict())
    return path


def _same_layout(a: TilerParquetMetadata, b: TilerParquetMetadata) -> bool:
    """Whether files written under ``a`` are still valid under ``b``: same
    grid and same variables."""
    return (
        a.n_i == b.n_i
        and a.n_j == b.n_j
        and a.lat == b.lat
        and a.lon == b.lon
        and set(a.variables) == set(b.variables)
    )


def _same_content(a: TilerParquetMetadata, b: TilerParquetMetadata) -> bool:
    return replace(a, generated_at="") == replace(b, generated_at="")


def _in_window(raw_timestamps: list, window_days: int | None) -> list:
    """The store's timestamps within ``window_days`` of its latest one."""
    if window_days is None or not raw_timestamps:
        return raw_timestamps
    start = max(raw_timestamps) - np.timedelta64(window_days, "D")
    return [t for t in raw_timestamps if t >= start]


def _sparse_rows_for_slice(arr: np.ndarray) -> pd.DataFrame:
    """One (lat, lon) slice -> sparse rows, finite values only."""
    finite = np.isfinite(arr)
    i_idx, j_idx = np.nonzero(finite)
    return pd.DataFrame(
        {
            "i": i_idx.astype(np.int32),
            "j": j_idx.astype(np.int32),
            "value": arr[finite].astype(np.float32),
        }
    )


def _fetch_batch(store: str, variables: list[str], batch_raw_ts: list) -> xr.Dataset:
    """One ``get_data`` call spanning ``batch_raw_ts``, computed eagerly.

    Mirrors ``slice_loader._fetch_slice_from_store``'s call shape but over a
    range instead of a single instant, so multiple output timestamps share one
    S3 round trip.
    """
    ds = get_datasource(store).get_data(
        date_start=_ts_for_get_data(batch_raw_ts[0]),
        date_end=_ts_for_get_data(batch_raw_ts[-1]),
    )
    ds = ds[variables]
    if "time" not in ds.dims:
        ds = ds.expand_dims("time")
    return ds.compute() if hasattr(ds, "compute") else ds


def sync_store(
    store: str,
    uuid: str,
    variables: list[str],
    output_dir: str,
    batch_days: int = 30,
    window_days: int | None = None,
    duckdb_config: TilerBatchDuckDBConfig | None = None,
) -> tuple[list[str], str]:
    """Bring ``store``'s parquet + sidecar up to date with its zarr.

    Only timestamps not already listed in the existing sidecar are converted;
    files already written are never rewritten. If the grid or variable set
    changed since the last run, the old timestamps are dropped from the
    sidecar and the window is converted again.

    ``window_days`` limits the run to timestamps within that many days of the
    store's latest one; None converts the full history. ``batch_days`` is
    how many timestamps one ``get_data`` call fetches.

    The sidecar is rewritten after every batch that added timestamps (so an
    interrupted run keeps its progress, and never lists a timestamp before
    its files exist), and otherwise only if its content changed.

    Returns ``(new timestamps written, metadata_path)``.
    """
    if duckdb_config is None:
        raise ValueError("duckdb_config is required")

    fresh = build_metadata(store, uuid, variables, timestamps=[])
    existing = read_metadata(output_dir, store)
    done: set[str] = set()
    if existing is not None:
        if _same_layout(existing, fresh):
            done = set(existing.timestamps)
        else:
            logger.warning(
                "Grid or variables of %s changed since the last run; "
                "converting its window again",
                store,
            )

    window = _in_window(list(get_store(store)["time"].values), window_days)
    pending = [t for t in window if _ts_native(t) not in done]
    logger.info(
        "Tiler parquet sync for %s: %d timestamp(s) in window, %d new",
        store,
        len(window),
        len(pending),
    )

    converted = set(done)
    written: list[str] = []
    metadata_path = store_metadata_path(output_dir, store)

    with TilerBatchDuckDBClient(duckdb_config) as client:
        for start in range(0, len(pending), batch_days):
            batch_raw_ts = pending[start : start + batch_days]
            written_before = len(written)
            ds = _fetch_batch(store, variables, batch_raw_ts)
            batch_ts_set = {_ts_native(t) for t in batch_raw_ts}
            # A long backfill can outlive the credentials the client started with.
            client.refresh_s3_secret()

            for k in range(ds.sizes["time"]):
                ts = _ts_native(ds["time"].values[k])
                if ts not in batch_ts_set:
                    # get_data's range can spill outside [batch_raw_ts[0], batch_raw_ts[-1]]
                    # if the store has no exact instant at the boundary.
                    continue
                frames = {
                    v: _sparse_rows_for_slice(ds[v].isel(time=k).values)
                    for v in variables
                }
                if all(f.empty for f in frames.values()):
                    # Nothing to draw; left unlisted so a later run checks again.
                    continue
                for v, frame in frames.items():
                    client.write_parquet(
                        frame, variable_parquet_path(output_dir, store, v, ts)
                    )
                converted.add(ts)
                written.append(ts)

            if len(written) > written_before:
                write_metadata(
                    replace(fresh, timestamps=sorted(converted)), output_dir, store
                )
            logger.info(
                "Tiler parquet sync for %s: %d/%d new timestamp(s) processed",
                store,
                min(start + batch_days, len(pending)),
                len(pending),
            )

    final = replace(fresh, timestamps=sorted(converted))
    if not written and (existing is None or not _same_content(existing, final)):
        write_metadata(final, output_dir, store)

    return written, metadata_path
