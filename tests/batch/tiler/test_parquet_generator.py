"""generate_parquet: sparse value parquet(s) + metadata sidecar from a zarr store.

Uses the same fake-ZarrDataSource pattern as
tests/batch/tiler/test_zarr_registry.py so no real S3 access is needed.
"""

import json

import duckdb
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


def test_generate_parquet_writes_sidecar_and_sparse_values(monkeypatch, tmp_path):
    times = [
        "2024-01-01T00:00:00",
        "2024-01-02T00:00:00",
        "2024-01-03T00:00:00",
        "2024-01-04T00:00:00",
    ]
    _patch_source(monkeypatch, _fake_dataset(times))

    value_paths, metadata_path = gen.generate_parquet(
        "s3://bucket/foo.zarr",
        "uuid-123",
        ["v", "flag"],
        str(tmp_path),
        batch_days=2,  # forces 2 batches over 4 timestamps
    )

    # Output lives under a subdirectory named after the zarr store, not the uuid.
    assert metadata_path == str(tmp_path / "foo" / "metadata.json")
    assert value_paths["v"] == str(tmp_path / "foo" / "v.parquet")
    assert value_paths["flag"] == str(tmp_path / "foo" / "flag.parquet")

    # --- sidecar ---
    with open(metadata_path) as f:
        meta = json.load(f)

    assert meta["uuid"] == "uuid-123"
    assert meta["dataset"] == "foo.zarr"
    assert meta["source_path"] == "s3://bucket/foo.zarr"
    assert meta["n_i"] == 2
    assert meta["n_j"] == 3
    assert meta["lat"] == [-40.0, -39.5]
    assert meta["lon"] == [110.0, 110.5, 111.0]
    assert len(meta["timestamps"]) == 4
    # str() of the store's own numpy.datetime64 value (not a pandas-trimmed
    # re-rendering of it), plus an explicit UTC marker.
    assert meta["timestamps"][0] == "2024-01-01T00:00:00.000000000Z"

    # dtype describes the parquet's value column (always float32), not the
    # source zarr variable's dtype (float64 for "v", int32 for "flag").
    assert meta["variables"]["v"]["dtype"] == "float32"
    assert meta["variables"]["v"]["attrs"]["units"] == "degree_C"
    assert meta["variables"]["flag"]["dtype"] == "float32"
    assert meta["variables"]["flag"]["attrs"]["flag_values"] == [0, 1]
    assert meta["variables"]["flag"]["attrs"]["flag_meanings"] == "none present"
    # Source-chunking attrs are dropped - meaningless once reshaped into sparse rows.
    assert "_ChunkSizes" not in meta["variables"]["v"]["attrs"]

    # --- value parquet: "v" (5 finite cells/frame x 4 frames = 20 rows) ---
    con = duckdb.connect(":memory:")
    v_rows = con.execute(
        f"SELECT * FROM read_parquet('{value_paths['v']}') ORDER BY timestamp, i, j"
    ).fetchall()
    assert len(v_rows) == 20
    # The known-NaN cell (i=0, j=1) never appears for "v".
    assert not any(r[4] == 0 and r[5] == 1 for r in v_rows)
    first = v_rows[0]
    assert first[0] == "2024-01-01T00:00:00.000000000Z"
    assert first[1] == "foo.zarr"
    assert first[2] == "uuid-123"
    assert first[3] == "v"
    assert first[4] == 0 and first[5] == 0
    assert first[6] == pytest.approx(0.0)

    # --- value parquet: "flag" (all 6 cells/frame x 4 frames = 24 rows) ---
    flag_rows = con.execute(
        f"SELECT dataset, uuid, variable, i, j, value "
        f"FROM read_parquet('{value_paths['flag']}') WHERE i = 1 AND j = 2"
    ).fetchall()
    assert len(flag_rows) == 4
    assert all(
        r[0] == "foo.zarr" and r[1] == "uuid-123" and r[2] == "flag" and r[5] == 1.0
        for r in flag_rows
    )


def test_generate_parquet_raises_for_unknown_variable(monkeypatch, tmp_path):
    _patch_source(monkeypatch, _fake_dataset(["2024-01-01T00:00:00"]))

    with pytest.raises(FileNotFoundError, match="NOT_A_REAL_VAR"):
        gen.generate_parquet(
            "s3://bucket/foo.zarr", "uuid-123", ["NOT_A_REAL_VAR"], str(tmp_path)
        )


def test_generate_parquet_respects_max_timestamps(monkeypatch, tmp_path):
    times = [f"2024-01-0{n}T00:00:00" for n in range(1, 5)]
    _patch_source(monkeypatch, _fake_dataset(times))

    _, metadata_path = gen.generate_parquet(
        "s3://bucket/foo.zarr",
        "uuid-123",
        ["v"],
        str(tmp_path),
        max_timestamps=2,
    )
    with open(metadata_path) as f:
        meta = json.load(f)
    # The sidecar's own timestamp list still reflects the full store...
    assert len(meta["timestamps"]) == 4

    # ...but only the most recent 2 were actually converted to value rows.
    con = duckdb.connect(":memory:")
    distinct_ts = con.execute(
        f"SELECT DISTINCT timestamp FROM read_parquet('{str(tmp_path)}/foo/v.parquet') "
        "ORDER BY timestamp"
    ).fetchall()
    assert [r[0] for r in distinct_ts] == [
        "2024-01-03T00:00:00.000000000Z",
        "2024-01-04T00:00:00.000000000Z",
    ]
