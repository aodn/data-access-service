"""build_metadata / _sparse_rows_for_slice / generate_parquet: the zarr ->
sparse-parquet conversion pipeline.

Uses the same fake-ZarrDataSource pattern as
tests/batch/tiler/test_zarr_registry.py so no real S3 access is needed for
reading zarr. Metadata content and sparse-row correctness are tested as pure
functions (no I/O at all); generate_parquet's own orchestration (paths, SQL
targeting) is tested with a stubbed TilerDuckDBClient and a mocked storage
module — no real S3/duckdb-httpfs needed for that either.
"""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.batch.tiler import parquet_generator as gen
from data_access_service.batch.tiler.zarr_registry import store_registry


class _FakeZarrSource:
    def __init__(self, ds: xr.Dataset):
        self.zarr_store = ds

    def get_data(self, date_start=None, date_end=None, **_kwargs) -> xr.Dataset:
        ds = self.zarr_store
        if date_start is not None or date_end is not None:
            return ds.sel(time=slice(date_start, date_end))
        return ds


def _patch_source(monkeypatch, ds: xr.Dataset):
    source = _FakeZarrSource(ds)
    monkeypatch.setattr(
        "data_access_service.batch.tiler.zarr_registry._resolve_zarr_source",
        lambda _url: source,
    )
    return source


@pytest.fixture(autouse=True)
def isolate_caches():
    store_registry.clear()
    yield
    store_registry.clear()


def _fake_dataset(times: list[str]) -> xr.Dataset:
    """4 timestamps, a 2x3 (lat, lon) grid, one continuous var with a fixed NaN
    cell and one categorical (CF flag_values) var with no NaN."""
    n_t = len(times)
    v = np.arange(n_t * 6, dtype=np.float64).reshape(n_t, 2, 3)
    v[:, 0, 1] = np.nan  # same cell every frame: (i=0, j=1)
    flag = np.zeros((n_t, 2, 3), dtype=np.int32)
    flag[:, 1, 2] = 1
    return xr.Dataset(
        {
            "v": xr.DataArray(
                v,
                dims=["time", "lat", "lon"],
                attrs={"units": "degree_C", "_ChunkSizes": [1, 2, 3]},
            ),
            "flag": xr.DataArray(
                flag,
                dims=["time", "lat", "lon"],
                attrs={
                    "flag_values": [0, 1],
                    "flag_meanings": "none present",
                },
            ),
        },
        coords={
            "time": pd.to_datetime(times),
            "lat": [-40.0, -39.5],
            "lon": [110.0, 110.5, 111.0],
        },
    )


# --- build_metadata -----------------------------------------------------


def test_build_metadata_grid_and_provenance(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    meta = gen.build_metadata("s3://bucket/foo.zarr", "uuid-123", ["v", "flag"])

    assert meta.uuid == "uuid-123"
    assert meta.dataset == "foo.zarr"
    assert meta.source_path == "s3://bucket/foo.zarr"
    assert meta.n_i == 2
    assert meta.n_j == 3
    assert meta.lat == [-40.0, -39.5]
    assert meta.lon == [110.0, 110.5, 111.0]
    assert len(meta.timestamps) == 1
    # str() of the store's own numpy.datetime64 value (not a pandas-trimmed
    # re-rendering of it), plus an explicit UTC marker.
    assert meta.timestamps[0] == "2024-01-01T00:00:00.000000000Z"


def test_build_metadata_variable_dtype_and_attrs(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    meta = gen.build_metadata("s3://bucket/foo.zarr", "uuid-123", ["v", "flag"])

    # dtype describes the parquet's value column (always float32), not the
    # source zarr variable's dtype (float64 for "v", int32 for "flag").
    assert meta.variables["v"].dtype == "float32"
    assert meta.variables["v"].attrs["units"] == "degree_C"
    assert meta.variables["flag"].dtype == "float32"
    assert meta.variables["flag"].attrs["flag_values"] == [0, 1]
    assert meta.variables["flag"].attrs["flag_meanings"] == "none present"
    # Source-chunking attrs are dropped - meaningless once reshaped into sparse rows.
    assert "_ChunkSizes" not in meta.variables["v"].attrs


def test_build_metadata_sets_parquet_path_per_variable(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    meta = gen.build_metadata("s3://bucket/foo.zarr", "uuid-123", ["v", "flag"])

    assert meta.variables["v"].parquet_path == "foo/v.parquet"
    assert meta.variables["flag"].parquet_path == "foo/flag.parquet"


def test_build_metadata_raises_for_unknown_variable(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    with pytest.raises(FileNotFoundError, match="NOT_A_REAL_VAR"):
        gen.build_metadata("s3://bucket/foo.zarr", "uuid-123", ["NOT_A_REAL_VAR"])


# --- _sparse_rows_for_slice -----------------------------------------------


def test_sparse_rows_for_slice_drops_nan_cells():
    arr = np.array([[0.0, np.nan], [2.0, 3.0]])
    rows = gen._sparse_rows_for_slice(
        arr, "2024-01-01T00:00:00.000000000Z", "d", "u", "v"
    )

    assert len(rows) == 3
    assert not ((rows["i"] == 0) & (rows["j"] == 1)).any()


def test_sparse_rows_for_slice_carries_identity_columns():
    arr = np.array([[1.0]])
    rows = gen._sparse_rows_for_slice(
        arr, "2024-01-01T00:00:00.000000000Z", "d.zarr", "u1", "v"
    )

    row = rows.iloc[0]
    assert row["timestamp"] == "2024-01-01T00:00:00.000000000Z"
    assert row["dataset"] == "d.zarr"
    assert row["uuid"] == "u1"
    assert row["variable"] == "v"
    assert row["i"] == 0 and row["j"] == 0
    assert row["value"] == pytest.approx(1.0)


def test_sparse_rows_for_slice_value_column_is_float32():
    arr = np.array([[1.0]], dtype=np.float64)
    rows = gen._sparse_rows_for_slice(
        arr, "2024-01-01T00:00:00.000000000Z", "d", "u", "v"
    )
    assert rows["value"].dtype == np.float32


def test_sparse_rows_for_slice_all_nan_is_empty():
    arr = np.array([[np.nan, np.nan]])
    rows = gen._sparse_rows_for_slice(
        arr, "2024-01-01T00:00:00.000000000Z", "d", "u", "v"
    )
    assert rows.empty


# --- generate_parquet orchestration (stubbed TilerDuckDBClient + storage) --


def _patch_duckdb_client(monkeypatch) -> MagicMock:
    """Stub gen.TilerDuckDBClient so generate_parquet's own-connection path
    never touches a real DuckDB/S3 connection. Returns the fake connection
    handed back by the stub's get_instance(), for callers to inspect."""
    con = MagicMock()

    class FakeTilerDuckDBClient:
        def __init__(self, config):
            self.config = config

        def get_instance(self):
            return con

    monkeypatch.setattr(gen, "TilerDuckDBClient", FakeTilerDuckDBClient)
    return con


def _run_generate_parquet(monkeypatch, ds, output_dir="s3://my-bucket/tiler", **kwargs):
    """generate_parquet with a stubbed TilerDuckDBClient and storage S3 calls
    — verifies generate_parquet's own control flow (paths, SQL targeting),
    not duckdb's real S3 write.
    """
    _patch_source(monkeypatch, ds)
    monkeypatch.setattr(gen.storage, "write_json", lambda path, data: None)
    con = _patch_duckdb_client(monkeypatch)
    return (
        gen.generate_parquet(
            "s3://bucket/foo.zarr",
            "uuid-123",
            ["v"],
            output_dir,
            duckdb_config=object(),
            **kwargs,
        ),
        con,
    )


def test_generate_parquet_requires_duckdb_config(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))
    monkeypatch.setattr(gen.storage, "write_json", lambda path, data: None)

    with pytest.raises(ValueError, match="duckdb_config"):
        gen.generate_parquet(
            "s3://bucket/foo.zarr", "uuid-123", ["v"], "s3://my-bucket/tiler"
        )


def test_generate_parquet_builds_own_client_from_duckdb_config(monkeypatch):
    """generate_parquet opens its own batch-tuned TilerDuckDBClient from
    ``duckdb_config`` - it never accepts a caller-supplied connection."""
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))
    monkeypatch.setattr(gen.storage, "write_json", lambda path, data: None)

    fake_con = MagicMock()
    calls = []

    class FakeClient:
        def __init__(self, config):
            calls.append(("init", config))

        def get_instance(self):
            return fake_con

    monkeypatch.setattr(gen, "TilerDuckDBClient", FakeClient)

    config = object()
    gen.generate_parquet(
        "s3://bucket/foo.zarr",
        "uuid-123",
        ["v"],
        "s3://my-bucket/tiler",
        duckdb_config=config,
    )

    assert ("init", config) in calls
    assert fake_con.execute.called


def test_generate_parquet_writes_sidecar_to_the_composed_s3_path(monkeypatch):
    write_json_calls = []
    monkeypatch.setattr(
        gen.storage, "write_json", lambda path, data: write_json_calls.append(path)
    )
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))
    _patch_duckdb_client(monkeypatch)

    _, metadata_path = gen.generate_parquet(
        "s3://bucket/foo.zarr",
        "uuid-123",
        ["v"],
        "s3://my-bucket/tiler",
        duckdb_config=object(),
    )

    assert metadata_path == "s3://my-bucket/tiler/foo/metadata.json"
    assert write_json_calls == ["s3://my-bucket/tiler/foo/metadata.json"]


def test_generate_parquet_copy_sql_targets_the_s3_value_path(monkeypatch):
    (value_paths, _), con = _run_generate_parquet(
        monkeypatch, _fake_dataset(["2024-01-01T00:00:00"])
    )

    assert value_paths["v"] == "s3://my-bucket/tiler/foo/v.parquet"
    copy_sql = [
        call.args[0] for call in con.execute.call_args_list if "COPY" in call.args[0]
    ]
    assert any("s3://my-bucket/tiler/foo/v.parquet" in sql for sql in copy_sql)


def test_generate_parquet_respects_max_timestamps(monkeypatch):
    times = [f"2024-01-0{n}T00:00:00" for n in range(1, 5)]
    seen_batches = []
    real_fetch_batch = gen._fetch_batch
    monkeypatch.setattr(
        gen,
        "_fetch_batch",
        lambda store_url, variables, batch_raw_ts: (
            seen_batches.append(list(batch_raw_ts))
            or real_fetch_batch(store_url, variables, batch_raw_ts)
        ),
    )

    (_, metadata_path), _ = _run_generate_parquet(
        monkeypatch, _fake_dataset(times), max_timestamps=2
    )

    # Only the most recent 2 timestamps were ever fetched/converted...
    assert len(seen_batches) == 1
    assert [gen._ts_native(t) for t in seen_batches[0]] == [
        "2024-01-03T00:00:00.000000000Z",
        "2024-01-04T00:00:00.000000000Z",
    ]
    # ...even though the sidecar's own timestamp list reflects the full store.
    assert metadata_path == "s3://my-bucket/tiler/foo/metadata.json"
