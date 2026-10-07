from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import replace
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import xarray as xr

from data_access_service.batch.tiler import storage
from data_access_service.batch.tiler.zarr_registry import get_datasource, get_store
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.duckdbclient import TilerBatchDuckDBClient
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    store_metadata_path,
    variable_parquet_path,
)
from data_access_service.models.tiler_types import TilerBatchDuckDBConfig
from data_access_service.utils.date_time_utils import time_it
from data_access_service.utils.s3_json import read_json

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
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def read_metadata(tiler_root_dir: str, store: str) -> TilerParquetMetadata | None:
    data = read_json(store_metadata_path(tiler_root_dir, store), required=False)
    return TilerParquetMetadata.from_dict(data) if data is not None else None


def write_metadata(
    aws: AWSHelper, meta: TilerParquetMetadata, tiler_root_dir: str, store: str
) -> str:
    path = store_metadata_path(tiler_root_dir, store)
    storage.write_json(aws, path, meta.to_dict())
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


# Rows are written block by block, BLOCK x BLOCK cells at a time, so each row
# group covers a small area and a bbox skips row groups in both directions.
# Nearby j values also compress better than whole rows of them.
BLOCK = 256


def _sparse_rows_for_slice(arr: np.ndarray) -> pd.DataFrame:
    """A (lat, lon) slice as (i, j, value) rows, NaNs dropped, in ``BLOCK``
    order (by (i, j) within a block)."""
    finite = np.isfinite(arr)
    i_idx, j_idx = np.nonzero(finite)
    value = arr[finite]
    # nonzero gives (i, j) order; a stable sort by block keeps it within each.
    blocks_across = -(-arr.shape[1] // BLOCK)
    block = (i_idx // BLOCK) * blocks_across + j_idx // BLOCK
    order = np.argsort(block.astype(np.int32), kind="stable")
    return pd.DataFrame(
        {
            "i": i_idx[order].astype(np.int32),
            "j": j_idx[order].astype(np.int32),
            "value": value[order].astype(np.float32),
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


def _open_batch(store: str, variables: list[str], batch_raw_ts: list) -> xr.Dataset:
    """``batch_raw_ts`` from the zarr, not read yet."""
    ds = get_datasource(store).get_data(
        date_start=_ts_for_get_data(batch_raw_ts[0]),
        date_end=_ts_for_get_data(batch_raw_ts[-1]),
    )
    ds = ds[variables]
    if "time" not in ds.dims:
        ds = ds.expand_dims("time")
    return ds


# About this much zarr data is read into memory at a time.
BAND_BYTES = 1024**3


def _band_rows(ds: xr.Dataset, variables: list[str]) -> int:
    """Lat rows read at a time: whole zarr lat chunks, about ``BAND_BYTES``."""
    row_bytes = sum(
        ds.sizes["time"] * ds.sizes["lon"] * ds[v].dtype.itemsize for v in variables
    )
    lat_chunks = [
        ds[v].encoding["chunks"][ds[v].dims.index("lat")]
        for v in variables
        if ds[v].encoding.get("chunks")
    ]
    lat_chunk = max(lat_chunks) if lat_chunks else 1
    rows = max(1, BAND_BYTES // (row_bytes * lat_chunk)) * lat_chunk
    return min(rows, ds.sizes["lat"])


def _whole_blocks(
    carry: np.ndarray, band: np.ndarray, band_start: int, last: bool
) -> tuple[list[tuple[int, np.ndarray]], np.ndarray]:
    """Split ``carry`` + ``band`` (rows from ``band_start``) into parts that
    cover whole ``BLOCK`` rows, as ``(first row, rows)``. The rows left over
    are returned to carry into the next band. ``carry`` starts on a
    ``BLOCK`` row, so every part does too and keeps the block order."""
    band_end = band_start + band.shape[0]
    end = band_end if last else band_end // BLOCK * BLOCK
    start = band_start - carry.shape[0]
    if end <= band_start:
        return [], np.concatenate([carry, band])
    # Only the block row the carry falls in is copied; the rest are views.
    head_end = min(-(-band_start // BLOCK) * BLOCK, end)
    parts = []
    if head_end > start:
        parts.append((start, np.concatenate([carry, band[: head_end - band_start]])))
    if end > head_end:
        parts.append((head_end, band[head_end - band_start : end - band_start]))
    return parts, band[end - band_start :].copy()


@time_it
def _read_band(
    ds: xr.Dataset, variable: str, band_start: int, band_rows: int
) -> xr.DataArray:
    """Read one latitude band of ``variable`` from the open zarr time chunk."""
    return ds[variable].isel(lat=slice(band_start, band_start + band_rows)).compute()


def _write_pieces(
    client: TilerBatchDuckDBClient,
    ds: xr.Dataset,
    variables: list[str],
    steps: list[int],
    piece_dir: str,
) -> tuple[
    dict[tuple[int, str], list[str]],
    dict[int, int],
    dict[tuple[int, str], int],
    float,
]:
    """Read ``ds`` one variable and one band of lat rows at a time and write
    each time step and variable as local parquet pieces, in row order. The
    next band is read while this one is written, so at most two bands are in
    memory.

    Returns ``(pieces by (step, variable), rows by step, rows by (step,
    variable), seconds spent reading bands)``.
    """
    n_i = ds.sizes["lat"]
    pieces: dict[tuple[int, str], list[str]] = {
        (k, v): [] for k in steps for v in variables
    }
    rows = dict.fromkeys(steps, 0)
    var_rows = {(k, v): 0 for k in steps for v in variables}
    carry = {
        (k, v): np.empty((0, ds.sizes["lon"]), dtype=ds[v].dtype)
        for k in steps
        for v in variables
    }
    # One variable at a time: one lat chunk of all variables together can be
    # well over BAND_BYTES (e.g. heatwave, 10 float64 variables).
    reads = [
        (v, band_start, band_rows)
        for v in variables
        for band_rows in [_band_rows(ds, [v])]
        for band_start in range(0, n_i, band_rows)
    ]

    read_seconds = 0.0

    def read(v: str, band_start: int, band_rows: int) -> xr.DataArray:
        # One reader thread, and the caller waits for it, so this add is safe.
        nonlocal read_seconds
        started = time.perf_counter()
        band = _read_band(ds, v, band_start, band_rows)
        read_seconds += time.perf_counter() - started
        return band

    if not reads:
        return pieces, rows, var_rows, read_seconds
    with ThreadPoolExecutor(max_workers=1) as reader:
        next_band = reader.submit(read, *reads[0])
        for n, (v, band_start, band_rows) in enumerate(reads):
            last = band_start + band_rows >= n_i
            # Drop the last band before taking the next, or three are held.
            band = None
            band = next_band.result()
            if n + 1 < len(reads):
                next_band = reader.submit(read, *reads[n + 1])
            for k in steps:
                parts, carry[k, v] = _whole_blocks(
                    carry[k, v], band.isel(time=k).values, band_start, last
                )
                for first_row, arr in parts:
                    frame = _sparse_rows_for_slice(arr)
                    frame["i"] += first_row
                    path = os.path.join(
                        piece_dir, f"{k}_{v}_{len(pieces[k, v])}.parquet"
                    )
                    client.write_parquet(frame, path)
                    pieces[k, v].append(path)
                    rows[k] += len(frame)
                    var_rows[k, v] += len(frame)
    return pieces, rows, var_rows, read_seconds


# Files uploaded at once, each on one connection (see storage.upload_file).
UPLOAD_WORKERS = 8


def _upload_and_remove(
    aws: AWSHelper, local_path: str, path: str, remove: set[str]
) -> None:
    """Upload ``local_path`` to ``path``, then delete ``remove`` to free the
    disk; a chunk's pieces add up to the whole chunk."""
    storage.upload_file(aws, local_path, path)
    for local in remove:
        with suppress(FileNotFoundError):
            os.remove(local)


def sync_store(
    store: str,
    uuid: str,
    variables: list[str],
    tiler_root_dir: str,
    max_chunks_per_run: int | None = None,
    duckdb_config: TilerBatchDuckDBConfig | None = None,
    regenerate_all: bool = False,
    skip_empty_variables: set[str] | None = None,
) -> tuple[list[str], str]:
    """Convert every zarr timestamp that has no parquet yet, one zarr time
    chunk at a time.

    - ``max_chunks_per_run`` caps the chunks per run (None: no cap).
    - Existing files are never rewritten; a grid or variable change starts
      the store over.
    - All-NaN timestamps are recorded as empty and not read again.
    - A variable in ``skip_empty_variables`` whose grid is all empty is not
      uploaded. The store timestamp is listed only when some file for it was
      uploaded. That variable's own timestamp list gains the instant only
      when its file was uploaded.
    - The sidecar is saved after each chunk, after its files.
    - ``regenerate_all`` ignores what is already in the bucket and converts
      every timestamp, overwriting existing files.

    Returns ``(timestamps written, metadata_path)``.
    """
    if duckdb_config is None:
        raise ValueError("duckdb_config is required")
    skip_empty = skip_empty_variables or set()

    fresh = build_metadata(store, uuid, variables, timestamps=[])
    existing = read_metadata(tiler_root_dir, store)
    converted: set[str] = set()
    empty: set[str] = set()
    # Instants whose file for that variable was uploaded. Independent of the
    # store timestamps list: a skipped variable does not gain the instant.
    variable_times: dict[str, set[str]] = {v: set() for v in variables}
    if existing is not None and not regenerate_all:
        if _same_layout(existing, fresh):
            converted = set(existing.timestamps)
            empty = set(existing.empty_timestamps)
            for name, meta in existing.variables.items():
                variable_times[name] = set(meta.timestamps)
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
        "Tiler parquet sync for %s (regenerate_all=%s): %d zarr chunk(s) to "
        "convert, %d this run",
        store,
        regenerate_all,
        len(missing),
        len(batches),
    )

    def current() -> TilerParquetMetadata:
        variable_meta = {
            name: replace(meta, timestamps=sorted(variable_times.get(name, set())))
            for name, meta in fresh.variables.items()
        }
        return replace(
            fresh,
            timestamps=sorted(converted),
            empty_timestamps=sorted(empty),
            variables=variable_meta,
        )

    written: list[str] = []
    sidecar_written = False
    metadata_path = store_metadata_path(tiler_root_dir, store)

    aws = AWSHelper()
    with (
        TilerBatchDuckDBClient(duckdb_config) as client,
        TemporaryDirectory() as tmp,
        ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as uploader,
    ):
        for n, batch_raw_ts in enumerate(batches, start=1):
            ds = _open_batch(store, variables, batch_raw_ts)
            batch_ts_set = {_ts_native(t) for t in batch_raw_ts}
            # get_data can return instants outside the batch.
            steps = {
                k: ts
                for k in range(ds.sizes["time"])
                if (ts := _ts_native(ds["time"].values[k])) in batch_ts_set
            }
            changed = bool(steps)

            with TemporaryDirectory(dir=tmp) as piece_dir:
                pieces, rows, var_rows, read_seconds = _write_pieces(
                    client, ds, variables, list(steps), piece_dir
                )
                logger.info(
                    "Tiler parquet sync for %s: chunk %d/%d read took %.6f seconds",
                    store,
                    n,
                    len(batches),
                    read_seconds,
                )
                uploads = []
                for k, ts in steps.items():
                    if rows[k] == 0:
                        empty.add(ts)
                        continue
                    for v in variables:
                        if var_rows[k, v] == 0 and v in skip_empty:
                            continue
                        local_path = pieces[k, v][0]
                        if len(pieces[k, v]) > 1:
                            local_path = os.path.join(piece_dir, f"{k}_{v}.parquet")
                            client.merge_parquet(pieces[k, v], local_path)
                        uploads.append(
                            uploader.submit(
                                _upload_and_remove,
                                aws,
                                local_path,
                                variable_parquet_path(tiler_root_dir, store, v, ts),
                                {local_path, *pieces[k, v]},
                            )
                        )
                        variable_times[v].add(ts)
                    converted.add(ts)
                    written.append(ts)
                # The sidecar lists these files, so they must be up first.
                for upload in uploads:
                    upload.result()

            if changed:
                write_metadata(aws, current(), tiler_root_dir, store)
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
        write_metadata(aws, current(), tiler_root_dir, store)

    return written, metadata_path
