from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

import duckdb
import numpy as np
import pandas as pd
import xarray as xr

from data_access_service import init_log
from data_access_service.batch.tiler import storage
from data_access_service.batch.tiler.zarr_registry import get_datasource, get_store
from data_access_service.config.config import Config
from data_access_service.models.estimation_types import schema_fingerprint
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    dataset_stem,
)

logger = logging.getLogger(__name__)

VALUE_COLUMNS = ("timestamp", "dataset", "uuid", "variable", "i", "j", "value")


VALUE_DTYPE = "float32"

_EXCLUDED_ATTRS = frozenset({"_ChunkSizes"})


def _dataset_key(store_url: str) -> str:
    """``s3://.../foo.zarr`` -> ``foo.zarr``, matching zarr_registry's own key."""
    return store_url.rstrip("/").rsplit("/", 1)[-1]


def _json_safe(value):
    """CF attrs sometimes carry numpy scalars/arrays; json.dump can't handle those."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _sql_literal(value: str) -> str:
    """Safely embed a string in SQL text.

    DuckDB's ``COPY ... TO`` does not accept a bound parameter for the target
    path (it silently writes nothing), so the path and the variable filter
    below are embedded as escaped literals rather than passed as params.
    """
    return "'" + value.replace("'", "''") + "'"


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
    store_url: str, uuid: str, variables: list[str]
) -> TilerParquetMetadata:
    """Grid + per-variable metadata for ``store_url``. Touches only coords/attrs,
    never the (potentially huge) value data.
    """
    store = get_store(store_url)  # normalised time/lat/lon view; coords are eager

    missing = [v for v in variables if v not in store.data_vars]
    if missing:
        raise FileNotFoundError(
            f"Variable(s) {missing} not found in store {store_url!r} "
            f"(available: {sorted(store.data_vars)})"
        )

    lat = store["lat"].values
    lon = store["lon"].values
    timestamps = [_ts_native(t) for t in store["time"].values]
    stem = dataset_stem(store_url)

    variable_meta = {
        v: TilerVariableMetadata(
            dtype=VALUE_DTYPE,
            attrs={
                k: _json_safe(val)
                for k, val in store[v].attrs.items()
                if k not in _EXCLUDED_ATTRS
            },
            parquet_path=f"{stem}/{v}.parquet",
        )
        for v in variables
    }

    return TilerParquetMetadata(
        uuid=uuid,
        dataset=_dataset_key(store_url),
        source_path=store_url,
        n_i=int(lat.shape[0]),
        n_j=int(lon.shape[0]),
        lat=[float(x) for x in lat],
        lon=[float(x) for x in lon],
        timestamps=timestamps,
        variables=variable_meta,
        schema_fingerprint=schema_fingerprint(variables),
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def write_metadata(meta: TilerParquetMetadata, dataset_dir: str) -> str:
    path = storage.join(dataset_dir, "metadata.json")
    storage.write_json(path, meta.to_dict())
    return path


def _sparse_rows_for_slice(
    arr: np.ndarray, timestamp: str, dataset: str, uuid: str, variable: str
) -> pd.DataFrame:
    """One (lat, lon) slice -> sparse rows, finite values only."""
    finite = np.isfinite(arr)
    i_idx, j_idx = np.nonzero(finite)
    return pd.DataFrame(
        {
            "timestamp": timestamp,
            "dataset": dataset,
            "uuid": uuid,
            "variable": variable,
            "i": i_idx.astype(np.int32),
            "j": j_idx.astype(np.int32),
            "value": arr[finite].astype(np.float32),
        }
    )


def _fetch_batch(
    store_url: str, variables: list[str], batch_raw_ts: list
) -> xr.Dataset:
    """One ``get_data`` call spanning ``batch_raw_ts``, computed eagerly.

    Mirrors ``slice_loader._fetch_slice_from_store``'s call shape but over a
    range instead of a single instant, so multiple output timestamps share one
    S3 round trip.
    """
    ds = get_datasource(store_url).get_data(
        date_start=_ts_for_get_data(batch_raw_ts[0]),
        date_end=_ts_for_get_data(batch_raw_ts[-1]),
    )
    ds = ds[variables]
    if "time" not in ds.dims:
        ds = ds.expand_dims("time")
    return ds.compute() if hasattr(ds, "compute") else ds


def generate_parquet(
    store_url: str,
    uuid: str,
    variables: list[str],
    output_dir: str,
    batch_days: int = 30,
    max_timestamps: int | None = None,
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> tuple[dict[str, str], str]:
    """Convert ``variables`` of ``store_url`` to sparse parquet + write the sidecar.

    Writes into ``{output_dir}/{dataset}/`` (``dataset`` = the zarr store's own
    name), so every parquet + the sidecar for one store live together.

    Returns ``({variable: parquet_path}, metadata_path)``.

    ``max_timestamps`` caps the run to the store's most recent N timestamps
    (local/dev sampling, or a "latest N" backfill); omit it for a full
    backfill. ``con`` lets a caller share one DuckDB connection across several
    stores instead of opening one per call.
    """
    meta = build_metadata(store_url, uuid, variables)
    dataset_dir = storage.join(output_dir, dataset_stem(store_url))
    metadata_path = write_metadata(meta, dataset_dir)

    raw_timestamps = list(get_store(store_url)["time"].values)
    if max_timestamps is not None:
        raw_timestamps = raw_timestamps[-max_timestamps:]
    if not raw_timestamps:
        raise ValueError(f"No timestamps to convert for {store_url!r}")

    own_con = con is None
    con = con or duckdb.connect(":memory:")
    storage.configure_s3(con, dataset_dir)
    table = f"tiler_rows_{abs(hash((store_url, tuple(variables))))}"

    try:
        con.execute(
            f"CREATE OR REPLACE TABLE {table} "
            "(timestamp VARCHAR, dataset VARCHAR, uuid VARCHAR, variable VARCHAR, "
            "i INTEGER, j INTEGER, value FLOAT)"
        )

        total = len(raw_timestamps)
        for start in range(0, total, batch_days):
            batch_raw_ts = raw_timestamps[start : start + batch_days]
            ds = _fetch_batch(store_url, variables, batch_raw_ts)
            batch_ts_set = {_ts_native(t) for t in batch_raw_ts}

            for k in range(ds.sizes["time"]):
                frame_ts = _ts_native(ds["time"].values[k])
                if frame_ts not in batch_ts_set:
                    # get_data's range can spill outside [batch_raw_ts[0], batch_raw_ts[-1]]
                    # if the store has no exact instant at the boundary.
                    continue
                for v in variables:
                    arr = ds[v].isel(time=k).values
                    rows = _sparse_rows_for_slice(arr, frame_ts, meta.dataset, uuid, v)
                    if not rows.empty:
                        con.execute(f"INSERT INTO {table} SELECT * FROM rows")

            logger.info(
                "tiler parquet generation: %s converted %d/%d timestamps",
                store_url,
                min(start + batch_days, total),
                total,
            )

        value_paths: dict[str, str] = {}
        for v in variables:
            path = storage.join(dataset_dir, f"{v}.parquet")
            con.execute(
                f"COPY (SELECT {', '.join(VALUE_COLUMNS)} FROM {table} "
                f"WHERE variable = {_sql_literal(v)} ORDER BY timestamp) "
                f"TO {_sql_literal(path)} (FORMAT PARQUET)"
            )
            value_paths[v] = path
    finally:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        if own_con:
            con.close()

    return value_paths, metadata_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("store_url", help="Zarr store URL, e.g. s3://bucket/foo.zarr")
    parser.add_argument(
        "uuid",
        help="Product/collection UUID, recorded in the sidecar for provenance "
        "(output is placed under a directory named after the zarr store, not this)",
    )
    parser.add_argument("variables", help="Comma-separated variable names")
    parser.add_argument(
        "output_dir",
        help="s3://bucket/prefix; a {dataset}/ subdirectory is created under it",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=30,
        help="Timestamps fetched per get_data call (default: 30)",
    )
    parser.add_argument(
        "--max-timestamps",
        type=int,
        default=None,
        help="Convert only the most recent N timestamps (default: all)",
    )
    args = parser.parse_args()

    config = Config.get_config()
    init_log(config)

    variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    value_paths, metadata_path = generate_parquet(
        args.store_url,
        args.uuid,
        variables,
        args.output_dir,
        batch_days=args.batch_days,
        max_timestamps=args.max_timestamps,
    )
    logger.info("Wrote sidecar: %s", metadata_path)
    for v, path in value_paths.items():
        logger.info("Wrote %s: %s", v, path)


if __name__ == "__main__":
    main()
