"""Precompute vector parquet from a gridded xarray.

One parquet per catalog UUID: columns ``timestamp, dataset, variable, i, j,
value``, written ``ORDER BY timestamp, i, j``. Colour is not stored. Native
grids larger than ``max_cells_long_edge`` are block-averaged so the file
stays small enough for the 8 GB API process.
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterator

import numpy as np
import pandas as pd
import xarray as xr

from data_access_service.config.config import Config
from data_access_service.core.api import BaseAPI
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_types import TilerVectorConfig
from data_access_service.utils.memory_utils import log_memory_usage

logger = logging.getLogger(__name__)


def _timestamp_key(value: Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y%m%dT%H%M%SZ")


def _block_reduce(arr: np.ndarray, factor: int) -> np.ndarray:
    ny, nx = arr.shape
    ny2, nx2 = ny // factor, nx // factor
    trimmed = arr[: ny2 * factor, : nx2 * factor]
    return trimmed.reshape(ny2, factor, nx2, factor).mean(axis=(1, 3))


def _downsample_slice(
    lat: np.ndarray, lon: np.ndarray, values: np.ndarray, max_edge: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    When converting large native spatial grids into parquet files for map tile generation, high-resolution grids
    can produce excessively large files. _downsample_slice caps the maximum grid dimension (longest of ny or nx)
    to a configurable size (max_edge, derived from max_cells_long_edge in configuration) by spatial block-averaging.
    This keeps memory consumption low and parquet output files small.
    :param lat:
    :param lon:
    :param values:
    :param max_edge:
    :return:
    """
    ny, nx = values.shape
    longest = max(ny, nx)
    if longest <= max_edge:
        return lat, lon, values
    factor = int(np.ceil(longest / max_edge))
    values = _block_reduce(values, factor)
    ny2, nx2 = values.shape
    lat = lat[: ny2 * factor].reshape(ny2, factor).mean(axis=1)
    lon = lon[: nx2 * factor].reshape(nx2, factor).mean(axis=1)
    return lat, lon, values


def _prepare_time_slice(
    timestamp: str,
    sl: xr.DataArray,
    lat_all: np.ndarray,
    lon_all: np.ndarray,
    max_edge: int,
) -> tuple[str, np.ndarray, np.ndarray, np.ndarray, pd.DataFrame]:
    """Compute one time step to a cell frame. Safe to run on a worker thread."""
    values = np.asarray(sl.values, dtype=np.float32)
    if values.ndim != 2:
        values = np.squeeze(values)
    if values.ndim != 2:
        raise ValueError(f"expected 2-D slice, got shape {values.shape}")
    lat, lon, values = _downsample_slice(lat_all, lon_all, values, max_edge)
    return timestamp, lat, lon, values, _cells_frame(values)


def _prepare_dataset_time_slice(
    timestamp: str,
    ds_slice: xr.Dataset,
    variables: list[str],
    lat_all: np.ndarray,
    lon_all: np.ndarray,
    max_edge: int,
) -> tuple[str, dict[str, tuple[np.ndarray, np.ndarray, np.ndarray, pd.DataFrame]]]:
    """Compute one time step across multiple variables. Safe to run on a worker thread."""
    results = {}
    for var in variables:
        sl = ds_slice[var]
        values = np.asarray(sl.values, dtype=np.float32)
        if values.ndim != 2:
            values = np.squeeze(values)
        if values.ndim != 2:
            raise ValueError(
                f"expected 2-D slice for variable {var}, got shape {values.shape}"
            )
        lat, lon, values = _downsample_slice(lat_all, lon_all, values, max_edge)
        results[var] = (lat, lon, values, _cells_frame(values))
    return timestamp, results


def _cells_frame(values: np.ndarray) -> pd.DataFrame:
    yy, xx = np.indices(values.shape, dtype=np.int32)
    flat = values.ravel()
    keep = np.isfinite(flat)
    return pd.DataFrame(
        {
            "i": yy.ravel()[keep],
            "j": xx.ravel()[keep],
            "value": flat[keep].astype(np.float32),
        }
    )


def _write_meta(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)


def local_meta_path(output_dir: str, uuid: str) -> str:
    return os.path.join(output_dir, f"{uuid}.meta.json")


def preprocess_dataset(
    ds: xr.Dataset,
    variables: list[str],
    *,
    uuid: str,
    dataset: str,
    client: TilerDuckDBClient | None = None,
    config: TilerVectorConfig | None = None,
) -> list[dict]:
    """Append time-slice parts for multiple variables in one pass. Caller must :meth:`finalize`."""
    cfg = config or Config.get_config().get_tiler_vector_config()
    own_client = client is None
    client = client or TilerDuckDBClient(cfg)
    if "lat" not in ds.dims or "lon" not in ds.dims:
        raise ValueError("Dataset must have lat and lon dimensions")

    if not variables:
        return []

    lat_all = np.asarray(ds["lat"].values, dtype=np.float64)
    lon_all = np.asarray(ds["lon"].values, dtype=np.float64)

    if "time" in ds.dims:
        jobs = [
            (_timestamp_key(t), ds.isel(time=i))
            for i, t in enumerate(ds["time"].values)
        ]
    else:
        jobs = [("na", ds)]

    cap = int(cfg.max_time_slices)
    if 0 < cap < len(jobs):
        logger.info(
            "Capping %s time slices to max_time_slices=%s",
            len(jobs),
            cap,
        )
        jobs = jobs[:cap]

    workers = max(1, int(cfg.threads))
    total = len(jobs)
    log_every = 1 if total <= 20 else min(50, max(1, total // 20))
    logger.info(
        "Computing %s time slice(s) for %s variable(s) (%s) with %s thread(s); "
        "first zarr chunk fetch can take a while",
        total,
        len(variables),
        variables,
        workers,
    )

    var_meta = {
        var: {
            "dataset": dataset,
            "variable": var,
            "n_i": 0,
            "n_j": 0,
            "lat_min": 0.0,
            "lat_max": 0.0,
            "lon_min": 0.0,
            "lon_max": 0.0,
            "vmin": None,
            "vmax": None,
            "timestamps": [],
        }
        for var in variables
    }

    def _run(job: tuple[str, xr.Dataset]):
        ts, sl = job
        return _prepare_dataset_time_slice(
            ts, sl, variables, lat_all, lon_all, cfg.max_cells_long_edge
        )

    def _iter_prepared() -> Iterator:
        if workers == 1 or total == 1:
            for job in jobs:
                yield _run(job)
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                yield from pool.map(_run, jobs)

    started = time.monotonic()
    try:
        for done, (timestamp, var_results) in enumerate(_iter_prepared(), start=1):
            for var, (lat, lon, values, frame) in var_results.items():
                client.write_part(uuid, timestamp, dataset, var, frame)
                meta = var_meta[var]
                meta["timestamps"].append(timestamp)
                meta["n_i"], meta["n_j"] = int(values.shape[0]), int(values.shape[1])
                meta["lat_min"], meta["lat_max"] = float(lat.min()), float(lat.max())
                meta["lon_min"], meta["lon_max"] = float(lon.min()), float(lon.max())
                finite = values[np.isfinite(values)]
                if finite.size:
                    lo, hi = float(finite.min()), float(finite.max())
                    meta["vmin"] = lo if meta["vmin"] is None else min(meta["vmin"], lo)
                    meta["vmax"] = hi if meta["vmax"] is None else max(meta["vmax"], hi)

            if done == 1 or done % log_every == 0 or done == total:
                elapsed = time.monotonic() - started
                logger.info(
                    "Vector slice %s/%s vars=%s ts=%s elapsed=%.1fs",
                    done,
                    total,
                    len(variables),
                    timestamp,
                    elapsed,
                )
    finally:
        if own_client:
            client.close()

    result_fragments = []
    for var in variables:
        meta = var_meta[var]
        result_fragments.append(
            {
                "dataset": meta["dataset"],
                "variable": meta["variable"],
                "n_i": meta["n_i"],
                "n_j": meta["n_j"],
                "lat_min": meta["lat_min"],
                "lat_max": meta["lat_max"],
                "lon_min": meta["lon_min"],
                "lon_max": meta["lon_max"],
                "vmin": 0.0 if meta["vmin"] is None else meta["vmin"],
                "vmax": 1.0 if meta["vmax"] is None else meta["vmax"],
                "timestamps": meta["timestamps"],
            }
        )

    return result_fragments


def preprocess_dataarray(
    da: xr.DataArray,
    *,
    uuid: str,
    dataset: str,
    variable: str | None = None,
    client: TilerDuckDBClient | None = None,
    config: TilerVectorConfig | None = None,
) -> dict:
    """Append time-slice parts for one variable. Caller must :meth:`finalize`."""
    var_name = variable or da.name or "value"
    ds = xr.Dataset({var_name: da})
    frags = preprocess_dataset(
        ds,
        [var_name],
        uuid=uuid,
        dataset=dataset,
        client=client,
        config=config,
    )
    return frags[0]


def _upload_local_file(local_path: str, bucket: str, key: str) -> None:
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")
    size = os.path.getsize(local_path)
    logger.info("Uploading %s (%s bytes) to s3://%s/%s", local_path, size, bucket, key)
    s3 = Config.get_config().get_s3_client()
    if s3 is None:
        raise RuntimeError("S3 client is not configured on Config")
    s3.upload_file(local_path, bucket, key)
    logger.info("Uploaded s3://%s/%s", bucket, key)


def _publish_meta(client: TilerDuckDBClient, uuid: str, payload: dict) -> None:
    local = local_meta_path(client._config.output_dir, uuid)
    os.makedirs(os.path.dirname(os.path.abspath(local)) or ".", exist_ok=True)
    _write_meta(local, payload)
    if not client._config.write_s3:
        return
    prefix = client._config.s3_prefix.strip("/")
    try:
        _upload_local_file(
            local, client._config.s3_bucket, f"{prefix}/{uuid}.meta.json"
        )
    except Exception:
        logger.exception("Meta JSON upload failed; file left at %s", local)
        if not client._config.keep_local_parquet:
            raise


def _publish_parquet(client: TilerDuckDBClient, uuid: str, local_path: str) -> str:
    """Upload the local merged parquet to S3 when configured; return the URI."""
    dest = client.parquet_uri(uuid)
    if not client._config.write_s3:
        logger.info("write_s3=false; parquet stays at %s", local_path)
        return dest
    try:
        _upload_local_file(local_path, client._config.s3_bucket, client.s3_key(uuid))
    except Exception:
        logger.exception("Parquet upload failed; local file left at %s", local_path)
        if not client._config.keep_local_parquet:
            raise
        return local_path
    if not client._config.keep_local_parquet:
        try:
            os.remove(local_path)
        except OSError:
            logger.warning("Could not remove temp parquet %s", local_path)
    return dest


def _zarr_work_list(api: BaseAPI, uuid: str | None) -> list[tuple[str, str]]:
    metadata_list = api.get_mapped_meta_data(uuid=None)
    work: list[tuple[str, str]] = []
    for catalog_uuid, datasets in sorted(metadata_list.items()):
        if uuid is not None and catalog_uuid != uuid:
            continue
        for dataset_name in datasets.keys():
            if dataset_name.endswith(".zarr"):
                work.append((catalog_uuid, dataset_name))
    return work


def _normalise_grid(
    ds: xr.Dataset, lat_name: str, lon_name: str, time_name: str | None
) -> xr.Dataset:
    rename: dict[str, str] = {}
    if lat_name != "lat":
        rename[lat_name] = "lat"
    if lon_name != "lon":
        rename[lon_name] = "lon"
    if time_name and time_name != "time":
        rename[time_name] = "time"
    return ds.rename(rename) if rename else ds


def _configured_gridded_variable_names() -> list[str]:
    """Scalar entries from ``tiler.gridded_variables`` (pairs are data-tile only)."""
    names: list[str] = []
    for spec in Config.get_tiler_gridded_variables() or []:
        if isinstance(spec, str):
            names.append(spec)
    return names


def _grid_variables(ds: xr.Dataset) -> list[str]:
    """Lat/lon data vars that appear in ``tiler.gridded_variables``, config order."""
    by_lower = {str(name).lower(): str(name) for name in ds.data_vars}
    picked: list[str] = []
    seen: set[str] = set()
    for spec in _configured_gridded_variable_names():
        actual = spec if spec in ds.data_vars else by_lower.get(spec.lower())
        if actual is None or actual in seen:
            continue
        dims = set(ds[actual].dims)
        if "lat" not in dims or "lon" not in dims:
            continue
        picked.append(actual)
        seen.add(actual)
    return picked


def _generate_for_zarr(
    api: BaseAPI,
    uuid: str,
    dataset_name: str,
    client: TilerDuckDBClient,
    config: TilerVectorConfig,
) -> list[dict]:
    """Open one zarr key and append parts. Returns per-variable meta fragments."""
    try:
        store = api._instance.get_dataset(dataset_name).zarr_store
    except Exception:
        logger.exception("Failed to open zarr uuid=%s dataset=%s", uuid, dataset_name)
        return []

    lat_name, lon_name, time_name = api.resolve_dim_names(uuid, dataset_name)
    if not lat_name or not lon_name:
        logger.warning(
            "Skip %s: no lat/lon dimensions (lat=%s lon=%s)",
            dataset_name,
            lat_name,
            lon_name,
        )
        return []

    ds = _normalise_grid(store, lat_name, lon_name, time_name)
    variables = _grid_variables(ds)
    if not variables:
        logger.warning(
            "Skip %s: no data variable matches tiler.gridded_variables "
            "(store has %s)",
            dataset_name,
            sorted(ds.data_vars),
        )
        return []
    logger.info(
        "Processing %s variables for %s: %s",
        len(variables),
        dataset_name,
        variables,
    )

    try:
        return preprocess_dataset(
            ds,
            variables,
            uuid=uuid,
            dataset=dataset_name,
            client=client,
            config=config,
        )
    except Exception:
        logger.exception(
            "Vector parquet failed uuid=%s dataset=%s variables=%s",
            uuid,
            dataset_name,
            variables,
        )
        return []


def generate_vector_parquet_for_zarrs(api: BaseAPI, uuid: str | None = None) -> None:
    """Write one parquet per catalog UUID (all zarr keys, all times).

    Args:
        api: Initialized API with metadata loaded.
        uuid: Optional catalog UUID. When set, only that UUID's ``.zarr``
            datasets are processed.
    """
    work = _zarr_work_list(api, uuid)
    if uuid is not None:
        logger.info(
            "Tiler vector batch restricted to uuid=%s (%s zarr dataset(s))",
            uuid,
            len(work),
        )
        if not work:
            logger.warning(
                "No zarr datasets found for uuid=%s; nothing to generate",
                uuid,
            )
            return
    else:
        logger.info(
            "Tiler vector batch for all UUIDs (%s zarr dataset(s))",
            len(work),
        )

    by_uuid: dict[str, list[str]] = {}
    for catalog_uuid, dataset_name in work:
        by_uuid.setdefault(catalog_uuid, []).append(dataset_name)

    cfg = Config.get_config().get_tiler_vector_config()
    os.makedirs(cfg.output_dir, exist_ok=True)
    logger.info("Vector working directory: %s", cfg.output_dir)

    for catalog_uuid, datasets in by_uuid.items():
        fragments: list[dict] = []
        with TilerDuckDBClient(cfg) as client:
            for dataset_name in datasets:
                fragments.extend(
                    _generate_for_zarr(api, catalog_uuid, dataset_name, client, cfg)
                )
                log_memory_usage(logger, f"after {dataset_name}")
            if not fragments:
                logger.warning(
                    "No vector parts for uuid=%s; skip finalize", catalog_uuid
                )
                continue
            local_dest = client.local_parquet_path(catalog_uuid)
            logger.info("Merging parts into %s (ORDER BY timestamp, i, j)", local_dest)
            local_parquet = client.finalize(catalog_uuid)
            size = (
                os.path.getsize(local_parquet) if os.path.isfile(local_parquet) else 0
            )
            logger.info("Merged parquet %s (%s bytes)", local_parquet, size)
            dest = _publish_parquet(client, catalog_uuid, local_parquet)
            timestamps = sorted(
                {ts for frag in fragments for ts in frag.get("timestamps", [])}
            )
            _publish_meta(
                client,
                catalog_uuid,
                {
                    "uuid": catalog_uuid,
                    "parquet": dest,
                    "variables": fragments,
                    "timestamps": timestamps,
                },
            )
            logger.info("Finalized vector parquet uuid=%s dest=%s", catalog_uuid, dest)
