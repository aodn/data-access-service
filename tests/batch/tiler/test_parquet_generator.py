"""build_metadata / _sparse_rows_for_slice / sync_store: the incremental
zarr -> sparse-parquet conversion.

Uses the same fake-ZarrDataSource pattern as
tests/batch/tiler/test_zarr_registry.py so no real S3 access is needed for
reading zarr. sync_store's orchestration (which timestamps, which paths,
sidecar updates) is tested with a stubbed TilerBatchDuckDBClient and an
in-memory stand-in for the S3 JSON reads and writes.
"""

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.batch.tiler import parquet_generator as gen
from data_access_service.batch.tiler.zarr_registry import close_all_stores


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
    close_all_stores()
    yield
    close_all_stores()


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

    meta = gen.build_metadata(
        "foo", "uuid-123", ["v", "flag"], ["2024-01-01T00:00:00.000000000Z"]
    )

    assert meta.uuid == "uuid-123"
    assert meta.dataset == "foo.zarr"
    assert meta.n_i == 2
    assert meta.n_j == 3
    assert meta.lat == [-40.0, -39.5]
    assert meta.lon == [110.0, 110.5, 111.0]
    assert meta.timestamps == ["2024-01-01T00:00:00.000000000Z"]


def test_build_metadata_variable_dtype_and_attrs(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    meta = gen.build_metadata("foo", "uuid-123", ["v", "flag"], [])

    # dtype describes the parquet's value column (always float32), not the
    # source zarr variable's dtype (float64 for "v", int32 for "flag").
    assert meta.variables["v"].dtype == "float32"
    assert meta.variables["v"].attrs["units"] == "degree_C"
    assert meta.variables["flag"].dtype == "float32"
    assert meta.variables["flag"].attrs["flag_values"] == [0, 1]
    assert meta.variables["flag"].attrs["flag_meanings"] == "none present"
    # Source-chunking attrs are dropped - meaningless once reshaped into sparse rows.
    assert "_ChunkSizes" not in meta.variables["v"].attrs


def test_build_metadata_raises_for_unknown_variable(monkeypatch):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    with pytest.raises(FileNotFoundError, match="NOT_A_REAL_VAR"):
        gen.build_metadata("foo", "uuid-123", ["NOT_A_REAL_VAR"], [])


# --- _sparse_rows_for_slice -----------------------------------------------


def test_sparse_rows_for_slice_drops_nan_cells():
    rows = gen._sparse_rows_for_slice(np.array([[0.0, np.nan], [2.0, 3.0]]))

    assert len(rows) == 3
    assert not ((rows["i"] == 0) & (rows["j"] == 1)).any()


def test_sparse_rows_for_slice_columns():
    rows = gen._sparse_rows_for_slice(np.array([[1.0]], dtype=np.float64))

    assert list(rows.columns) == ["i", "j", "value"]
    assert rows["value"].dtype == np.float32
    assert rows.iloc[0]["value"] == pytest.approx(1.0)


def test_sparse_rows_for_slice_all_nan_is_empty():
    assert gen._sparse_rows_for_slice(np.array([[np.nan, np.nan]])).empty


def test_sparse_rows_for_slice_writes_block_by_block(monkeypatch):
    """Each block's rows come together, blocks in (i, j) order, and (i, j)
    order within a block - so a row group covers a small area."""
    monkeypatch.setattr(gen, "BLOCK", 2)
    arr = np.arange(3 * 5, dtype=np.float64).reshape(3, 5)
    arr[1, 3] = np.nan

    rows = gen._sparse_rows_for_slice(arr)

    got = list(zip(rows["i"], rows["j"]))
    block_00 = [(0, 0), (0, 1), (1, 0), (1, 1)]
    block_01 = [(0, 2), (0, 3), (1, 2)]  # (1, 3) is NaN
    block_02 = [(0, 4), (1, 4)]
    blocks_1x = [(2, 0), (2, 1), (2, 2), (2, 3), (2, 4)]
    assert got == block_00 + block_01 + block_02 + blocks_1x
    # Values stay with their cells.
    assert list(rows["value"]) == [arr[i, j] for i, j in got]


# --- sync_store (stubbed TilerBatchDuckDBClient + storage) -------------------

OUTPUT_DIR = "s3://my-bucket/tiler"
SIDECAR = f"{OUTPUT_DIR}/foo/metadata.json"


class _Env:
    """In-memory S3 JSON plus a fake client, recording one ordered event log
    of parquet and sidecar writes."""

    def __init__(self, monkeypatch):
        self.json: dict[str, dict] = {}
        self.events: list[tuple[str, str]] = []
        self.client = MagicMock()
        self.client.__enter__.return_value = self.client
        self.client.write_parquet.side_effect = lambda frame, path: (
            self.events.append(("parquet", path))
        )
        monkeypatch.setattr(gen, "read_json", lambda path, **_: self.json.get(path))
        monkeypatch.setattr(gen.storage, "write_json", self._write_json)
        monkeypatch.setattr(gen, "TilerBatchDuckDBClient", lambda config: self.client)

    def _write_json(self, path, data):
        self.events.append(("json", path))
        self.json[path] = data

    def parquet_paths(self) -> list[str]:
        return [p for kind, p in self.events if kind == "parquet"]

    def sidecar_timestamps(self) -> list[str]:
        return self.json[SIDECAR]["timestamps"]


def _sync(monkeypatch, ds, variables=("v",), **kwargs):
    close_all_stores()
    _patch_source(monkeypatch, ds)
    return gen.sync_store(
        "foo",
        "uuid-123",
        list(variables),
        OUTPUT_DIR,
        duckdb_config=object(),
        **kwargs,
    )


def _ts(day: int) -> str:
    return f"2024-01-0{day}T00:00:00.000000000Z"


DAYS = [f"2024-01-0{n}T00:00:00" for n in range(1, 4)]


def test_sync_store_requires_duckdb_config(monkeypatch):
    _Env(monkeypatch)
    _patch_source(monkeypatch, _fake_dataset(DAYS))

    with pytest.raises(ValueError, match="duckdb_config"):
        gen.sync_store("foo", "uuid-123", ["v"], OUTPUT_DIR)


def test_first_run_writes_one_file_per_variable_per_timestamp(monkeypatch):
    env = _Env(monkeypatch)

    written, metadata_path = _sync(monkeypatch, _fake_dataset(DAYS), ("v", "flag"))

    assert written == [_ts(3), _ts(2), _ts(1)]
    assert metadata_path == SIDECAR
    assert f"{OUTPUT_DIR}/foo/v/2024-01-01T000000.000000000Z.parquet" in (
        env.parquet_paths()
    )
    assert len(env.parquet_paths()) == 2 * 3
    assert env.sidecar_timestamps() == [_ts(1), _ts(2), _ts(3)]
    assert env.client.__exit__.called


def test_rerun_with_no_new_timestamps_writes_nothing(monkeypatch):
    env = _Env(monkeypatch)
    _sync(monkeypatch, _fake_dataset(DAYS))
    env.events.clear()

    written, _ = _sync(monkeypatch, _fake_dataset(DAYS))

    assert written == []
    assert env.events == []


def test_new_timestamps_are_appended_without_rewriting_old_ones(monkeypatch):
    env = _Env(monkeypatch)
    _sync(monkeypatch, _fake_dataset(DAYS[:2]))
    env.events.clear()

    written, _ = _sync(monkeypatch, _fake_dataset(DAYS))

    assert written == [_ts(3)]
    assert env.parquet_paths() == [
        f"{OUTPUT_DIR}/foo/v/2024-01-03T000000.000000000Z.parquet"
    ]
    assert env.sidecar_timestamps() == [_ts(1), _ts(2), _ts(3)]


def test_max_chunks_per_run_caps_a_run_newest_first(monkeypatch):
    env = _Env(monkeypatch)

    written, _ = _sync(monkeypatch, _fake_dataset(DAYS), max_chunks_per_run=1)

    assert written == [_ts(3)]
    assert env.sidecar_timestamps() == [_ts(3)]


def test_capped_runs_carry_on_until_nothing_is_missing(monkeypatch):
    env = _Env(monkeypatch)
    _sync(monkeypatch, _fake_dataset(DAYS), max_chunks_per_run=1)

    assert _sync(monkeypatch, _fake_dataset(DAYS), max_chunks_per_run=1)[0] == [_ts(2)]
    assert _sync(monkeypatch, _fake_dataset(DAYS), max_chunks_per_run=1)[0] == [_ts(1)]

    env.events.clear()
    assert _sync(monkeypatch, _fake_dataset(DAYS), max_chunks_per_run=1)[0] == []
    assert env.events == []


def _chunked(ds: xr.Dataset, time_chunk: int) -> xr.Dataset:
    for v in ds.data_vars:
        ds[v].encoding["chunks"] = (time_chunk, 2, 3)
    return ds


def test_sidecar_is_written_after_the_files_it_lists(monkeypatch):
    env = _Env(monkeypatch)

    _sync(monkeypatch, _chunked(_fake_dataset(DAYS), 2))

    # Zarr chunks [1-2] [3]: file for ts 3, sidecar, files for ts 1-2, sidecar.
    kinds = [kind for kind, _ in env.events]
    assert kinds == ["parquet", "json", "parquet", "parquet", "json"]


def test_each_zarr_time_chunk_is_read_once(monkeypatch):
    env = _Env(monkeypatch)
    days = [f"2024-01-0{n}T00:00:00" for n in range(1, 8)]
    fetched = []
    real_fetch = gen._fetch_batch
    monkeypatch.setattr(
        gen,
        "_fetch_batch",
        lambda store, variables, ts: fetched.append(len(ts))
        or real_fetch(store, variables, ts),
    )

    # Chunks of 3 over 7 days: [1-3] [4-6] [7]; the latest first, then
    # history newest first.
    _sync(monkeypatch, _chunked(_fake_dataset(days), 3))

    assert fetched == [1, 3, 3]
    assert len(env.sidecar_timestamps()) == 7


def test_time_chunk_size_takes_the_smallest_across_variables():
    ds = _chunked(_fake_dataset(DAYS), 3)
    ds["flag"].encoding["chunks"] = (2, 2, 3)

    assert gen._time_chunk_size(ds, ["v", "flag"]) == 2
    assert gen._time_chunk_size(ds, ["v"]) == 3


def test_time_chunk_size_is_one_without_chunk_encoding():
    assert gen._time_chunk_size(_fake_dataset(DAYS), ["v"]) == 1


def test_missing_by_chunk_skips_handled_timestamps():
    times = list(pd.to_datetime([f"2024-01-0{n}" for n in range(1, 8)]).values)
    handled = {gen._ts_native(times[k]) for k in (0, 3, 4)}

    batches = gen._missing_by_chunk(times, handled, chunk_size=3)

    # Chunks [1-3] [4-6] [7], newest first, minus days 1, 4 and 5.
    assert batches == [[times[6]], [times[5]], [times[1], times[2]]]


def test_s3_secret_is_refreshed_before_writing(monkeypatch):
    env = _Env(monkeypatch)

    _sync(monkeypatch, _fake_dataset(DAYS))

    names = [c[0] for c in env.client.method_calls]
    assert names.index("refresh_s3_secret") < names.index("write_parquet")


def test_all_empty_timestamp_is_recorded_and_not_read_again(monkeypatch):
    env = _Env(monkeypatch)
    ds = _fake_dataset(DAYS)
    ds["v"][1] = np.nan

    written, _ = _sync(monkeypatch, ds)

    assert written == [_ts(3), _ts(1)]
    assert env.sidecar_timestamps() == [_ts(1), _ts(3)]
    assert env.json[SIDECAR]["empty_timestamps"] == [_ts(2)]

    env.events.clear()
    monkeypatch.setattr(gen, "_fetch_batch", MagicMock())
    written, _ = _sync(monkeypatch, ds)
    assert written == []
    assert env.events == []


def test_grid_change_converts_the_store_again(monkeypatch):
    env = _Env(monkeypatch)
    _sync(monkeypatch, _fake_dataset(DAYS))
    env.events.clear()

    ds = _fake_dataset(DAYS).assign_coords(lat=[-41.0, -40.5])
    written, _ = _sync(monkeypatch, ds)

    assert written == [_ts(3), _ts(2), _ts(1)]
    assert env.json[SIDECAR]["lat"] == [-41.0, -40.5]


def test_attr_change_alone_rewrites_the_sidecar(monkeypatch):
    env = _Env(monkeypatch)
    _sync(monkeypatch, _fake_dataset(DAYS))
    env.events.clear()

    ds = _fake_dataset(DAYS)
    ds["v"].attrs["units"] = "K"
    written, _ = _sync(monkeypatch, ds)

    assert written == []
    assert env.events == [("json", SIDECAR)]
    assert env.json[SIDECAR]["variables"]["v"]["attrs"]["units"] == "K"
