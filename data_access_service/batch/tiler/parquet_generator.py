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
    """numpy values -> plain Python, for JSON."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _ts_native(ts) -> str:
    return f"{ts}Z"


def _ts_for_get_data(ts) -> str:
    """Date bound for ``ZarrDataSource.get_data``."""
    return pd.Timestamp(ts).isoformat()


def build_metadata(
    store: str, uuid: str, variables: list[str], timestamps: list[str]
) -> TilerParquetMetadata:
    """The sidecar for ``store``: grid, variable attrs and ``timestamps``.
    Reads coords and attrs only."""
    ds = get_store(store)

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


def read_metadata(tiler_root_dir: str, store: str) -> TilerParquetMetadata | None:
    data = storage.read_json(store_metadata_path(tiler_root_dir, store))
    return TilerParquetMetadata.from_dict(data) if data is not None else None


def write_metadata(meta: TilerParquetMetadata, tiler_root_dir: str, store: str) -> str:
    path = store_metadata_path(tiler_root_dir, store)
    storage.write_json(path, meta.to_dict())
    return path


def _same_layout(a: TilerParquetMetadata, b: TilerParquetMetadata) -> bool:
    """Same grid and variables, so files written under ``a`` still fit ``b``."""
    return (
        a.n_i == b.n_i
        and a.n_j == b.n_j
        and a.lat == b.lat
        and a.lon == b.lon
        and set(a.variables) == set(b.variables)
    )


def _same_content(a: TilerParquetMetadata, b: TilerParquetMetadata) -> bool:
    return replace(a, generated_at="") == replace(b, generated_at="")


def _sparse_rows_for_slice(arr: np.ndarray) -> pd.DataFrame:
    """A (lat, lon) slice as (i, j, value) rows, NaNs dropped."""
    finite = np.isfinite(arr)
    i_idx, j_idx = np.nonzero(finite)
    return pd.DataFrame(
        {
            "i": i_idx.astype(np.int32),
            "j": j_idx.astype(np.int32),
            "value": arr[finite].astype(np.float32),
        }
    )


def _time_chunk_size(ds: xr.Dataset, variables: list[str]) -> int:
    """The zarr time chunk size (smallest across ``variables``, 1 if unknown)."""
    sizes = [
        ds[v].encoding["chunks"][ds[v].dims.index("time")]
        for v in variables
        if ds[v].encoding.get("chunks")
    ]
    return min(sizes) if sizes else 1


def _missing_by_chunk(
    all_times: list, handled: set[str], chunk_size: int
) -> list[list]:
    """Unhandled timestamps grouped by zarr time chunk, newest chunk first."""
    chunks: dict[int, list] = {}
    for k, t in enumerate(all_times):
        if _ts_native(t) not in handled:
            chunks.setdefault(k // chunk_size, []).append(t)
    return [chunks[c] for c in sorted(chunks, reverse=True)]


def _fetch_batch(store: str, variables: list[str], batch_raw_ts: list) -> xr.Dataset:
    """Read ``batch_raw_ts`` from the zarr into memory."""
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
    tiler_root_dir: str,
    max_chunks_per_run: int | None = None,
    duckdb_config: TilerBatchDuckDBConfig | None = None,
) -> tuple[list[str], str]:
    """Convert every zarr timestamp that has no parquet yet, one zarr time
    chunk at a time.

    - ``max_chunks_per_run`` caps the chunks per run (None: no cap).
    - Existing files are never rewritten; a grid or variable change starts
      the store over.
    - All-NaN timestamps are recorded as empty and not read again.
    - The sidecar is saved after each chunk, after its files.

    Returns ``(timestamps written, metadata_path)``.
    """
    if duckdb_config is None:
        raise ValueError("duckdb_config is required")

    fresh = build_metadata(store, uuid, variables, timestamps=[])
    existing = read_metadata(tiler_root_dir, store)
    converted: set[str] = set()
    empty: set[str] = set()
    if existing is not None:
        if _same_layout(existing, fresh):
            converted = set(existing.timestamps)
            empty = set(existing.empty_timestamps)
        else:
            logger.warning(
                "Grid or variables of %s changed since the last run; "
                "converting it again",
                store,
            )

    ds_store = get_store(store)
    missing = _missing_by_chunk(
        list(ds_store["time"].values),
        converted | empty,
        _time_chunk_size(ds_store, variables),
    )
    batches = missing if max_chunks_per_run is None else missing[:max_chunks_per_run]
    logger.info(
        "Tiler parquet sync for %s: %d zarr chunk(s) with missing timestamps, "
        "%d this run",
        store,
        len(missing),
        len(batches),
    )

    def current() -> TilerParquetMetadata:
        return replace(
            fresh, timestamps=sorted(converted), empty_timestamps=sorted(empty)
        )

    written: list[str] = []
    sidecar_written = False
    metadata_path = store_metadata_path(tiler_root_dir, store)

    with TilerBatchDuckDBClient(duckdb_config) as client:
        for n, batch_raw_ts in enumerate(batches, start=1):
            changed = False
            ds = _fetch_batch(store, variables, batch_raw_ts)
            batch_ts_set = {_ts_native(t) for t in batch_raw_ts}
            # Long runs can outlive the S3 credentials.
            client.refresh_s3_secret()

            for k in range(ds.sizes["time"]):
                ts = _ts_native(ds["time"].values[k])
                if ts not in batch_ts_set:
                    # get_data can return instants outside the batch.
                    continue
                changed = True
                frames = {
                    v: _sparse_rows_for_slice(ds[v].isel(time=k).values)
                    for v in variables
                }
                if all(f.empty for f in frames.values()):
                    empty.add(ts)
                    continue
                for v, frame in frames.items():
                    client.write_parquet(
                        frame, variable_parquet_path(tiler_root_dir, store, v, ts)
                    )
                converted.add(ts)
                written.append(ts)

            if changed:
                write_metadata(current(), tiler_root_dir, store)
                sidecar_written = True
            logger.info(
                "Tiler parquet sync for %s: %d/%d chunk(s) processed",
                store,
                n,
                len(batches),
            )

    if not sidecar_written and (
        existing is None or not _same_content(existing, current())
    ):
        write_metadata(current(), tiler_root_dir, store)

    return written, metadata_path
