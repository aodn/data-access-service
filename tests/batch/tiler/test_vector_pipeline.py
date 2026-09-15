"""Minimal vector preprocess + on-the-fly colour → PNG/WebP."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.batch.tiler import generator as tiler_generator
from data_access_service.batch.tiler.generator import (
    _grid_variables,
    _publish_meta,
    _timestamp_key,
    generate_vector_parquet_for_zarrs,
    local_meta_path,
    preprocess_dataarray,
)
from data_access_service.batch.tiler.render import lut_from_name, render_time_slice
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_types import TilerVectorConfig


@pytest.fixture
def vector_cfg(tmp_path: Path) -> TilerVectorConfig:
    return TilerVectorConfig(
        duckdb_database=":memory:",
        duckdb_temp_dir=str(tmp_path / "duckdb_tmp"),
        memory_limit="128MB",
        threads=2,
        region="ap-southeast-2",
        output_dir=str(tmp_path / "out"),
        max_cells_long_edge=32,
        s3_prefix="tiler",
        s3_bucket="test-site-snapshot-bucket",
        write_s3=False,
        keep_local_parquet=True,
        row_group_size=8192,
        max_time_slices=0,
    )


@pytest.fixture
def client(vector_cfg: TilerVectorConfig):
    with TilerDuckDBClient(vector_cfg) as c:
        yield c


def _grid() -> xr.DataArray:
    time = pd.date_range("2024-06-01", periods=2, freq="D")
    lat = np.linspace(-5.0, 5.0, 8)
    lon = np.linspace(150.0, 160.0, 10)
    data = np.arange(2 * 8 * 10, dtype=np.float32).reshape(2, 8, 10)
    return xr.DataArray(
        data,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": lat, "lon": lon},
        name="sst",
    )


def test_tiler_vector_config_from_yaml():
    from data_access_service.config.config import Config, EnvType

    cfg = Config.get_config(EnvType.TESTING).get_tiler_vector_config()
    assert cfg.duckdb_database == ":memory:"
    assert cfg.memory_limit == "128MB"
    assert cfg.max_cells_long_edge == 32
    assert cfg.threads == 2
    assert cfg.s3_prefix == "tiler"
    assert cfg.write_s3 is False
    assert cfg.keep_local_parquet is True
    assert cfg.max_time_slices == 0
    assert cfg.s3_bucket == "test-site-snapshot-bucket"
    assert cfg.output_dir == os.path.join(tempfile.gettempdir(), "tiler_vector_out")


def _finalize(client, vector_cfg, uuid: str, fragment: dict) -> str:
    dest = client.finalize(uuid)
    _publish_meta(
        client,
        uuid,
        {
            "uuid": uuid,
            "parquet": dest,
            "variables": [fragment],
            "timestamps": fragment["timestamps"],
        },
    )
    return dest


def test_preprocess_writes_one_parquet_ordered_by_time(client, vector_cfg):
    da = _grid()
    frag = preprocess_dataarray(
        da,
        uuid="demo",
        dataset="demo.zarr",
        variable="sst",
        client=client,
        config=vector_cfg,
    )
    dest = _finalize(client, vector_cfg, "demo", frag)
    assert Path(dest).is_file()
    assert dest.endswith("demo.parquet")
    meta = json.loads(Path(local_meta_path(vector_cfg.output_dir, "demo")).read_text())
    assert meta["variables"][0]["n_i"] == 8
    assert meta["variables"][0]["n_j"] == 10
    ordered = (
        client.execute(f"SELECT timestamp FROM read_parquet('{dest}')")
        .fetchdf()["timestamp"]
        .tolist()
    )
    assert ordered == sorted(ordered)
    assert frag["timestamps"] == [
        _timestamp_key(da["time"].values[0]),
        _timestamp_key(da["time"].values[1]),
    ]


def test_read_window_returns_subset(client, vector_cfg):
    da = _grid()
    frag = preprocess_dataarray(
        da,
        uuid="demo",
        dataset="demo.zarr",
        variable="sst",
        client=client,
        config=vector_cfg,
    )
    _finalize(client, vector_cfg, "demo", frag)
    ts = _timestamp_key(da["time"].values[0])
    all_rows = client.read_cells("demo", ts, variable="sst")
    window = client.read_cells(
        "demo", ts, i_min=0, i_max=1, j_min=0, j_max=1, variable="sst"
    )
    assert len(all_rows) == 80
    assert len(window) == 4
    assert set(window.columns) == {"i", "j", "value"}


def test_render_colormap_changes_bytes_not_parquet(client, vector_cfg):
    da = _grid()
    frag = preprocess_dataarray(
        da,
        uuid="demo",
        dataset="demo.zarr",
        variable="sst",
        client=client,
        config=vector_cfg,
    )
    _finalize(client, vector_cfg, "demo", frag)
    ts = _timestamp_key(da["time"].values[0])
    png_viridis = render_time_slice(
        client, "demo", ts, variable="sst", colormap="viridis", fmt="png"
    )
    png_gray = render_time_slice(
        client, "demo", ts, variable="sst", colormap="gray", fmt="png"
    )
    webp = render_time_slice(
        client, "demo", ts, variable="sst", colormap="viridis", fmt="webp"
    )
    assert png_viridis[:8] == b"\x89PNG\r\n\x1a\n"
    assert webp[:4] == b"RIFF"
    assert png_viridis != png_gray
    assert lut_from_name("gray")[255].tolist() == [255, 255, 255, 255]


def test_downsample_caps_long_edge(client, vector_cfg):
    cfg = TilerVectorConfig(**{**vector_cfg.__dict__, "max_cells_long_edge": 4})
    time = pd.date_range("2024-01-01", periods=1, freq="D")
    lat = np.linspace(-10, 10, 20)
    lon = np.linspace(140, 160, 40)
    da = xr.DataArray(
        np.ones((1, 20, 40), dtype=np.float32),
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": lat, "lon": lon},
        name="sst",
    )
    frag = preprocess_dataarray(
        da,
        uuid="big",
        dataset="big.zarr",
        variable="sst",
        client=client,
        config=cfg,
    )
    assert max(frag["n_i"], frag["n_j"]) <= 4


def test_grid_variables_only_picks_configured_names(monkeypatch):
    monkeypatch.setattr(
        tiler_generator.Config,
        "get_tiler_gridded_variables",
        staticmethod(lambda: ["sst", ["UCUR", "VCUR"], "analysed_sst"]),
    )
    lat = np.linspace(-1, 1, 4)
    lon = np.linspace(140, 141, 4)
    ds = xr.Dataset(
        {
            "dt_analysis": (("lat", "lon"), np.ones((4, 4), dtype=np.float32)),
            "sst": (("lat", "lon"), np.zeros((4, 4), dtype=np.float32)),
            "analysed_sst": (("lat", "lon"), np.ones((4, 4), dtype=np.float32)),
        },
        coords={"lat": lat, "lon": lon},
    )
    assert _grid_variables(ds) == ["sst", "analysed_sst"]


def test_max_time_slices_caps_jobs(client, vector_cfg):
    cfg = TilerVectorConfig(**{**vector_cfg.__dict__, "max_time_slices": 1})
    da = _grid()
    frag = preprocess_dataarray(
        da,
        uuid="capped",
        dataset="capped.zarr",
        variable="sst",
        client=client,
        config=cfg,
    )
    assert frag["timestamps"] == [_timestamp_key(da["time"].values[0])]


def test_generate_vector_parquet_restricts_to_uuid(monkeypatch):
    api = MagicMock()
    api.get_mapped_meta_data.return_value = {
        "uuid-a": {"a.zarr": {}, "notes.parquet": {}},
        "uuid-b": {"b.zarr": {}},
    }
    calls: list[tuple[str, str]] = []

    def fake_one(_api, catalog_uuid, dataset_name, _client=None, _cfg=None):
        calls.append((catalog_uuid, dataset_name))
        return []

    monkeypatch.setattr(tiler_generator, "_generate_for_zarr", fake_one)
    monkeypatch.setattr(tiler_generator, "log_memory_usage", lambda *a, **k: None)

    generate_vector_parquet_for_zarrs(api, uuid="uuid-a")
    assert calls == [("uuid-a", "a.zarr")]


def test_generate_vector_parquet_all_uuids_zarr_only(monkeypatch):
    api = MagicMock()
    api.get_mapped_meta_data.return_value = {
        "uuid-a": {"a.zarr": {}, "skip.parquet": {}},
        "uuid-b": {"b.zarr": {}},
    }
    calls: list[tuple[str, str]] = []

    def fake_one(_api, catalog_uuid, dataset_name, _client=None, _cfg=None):
        calls.append((catalog_uuid, dataset_name))
        return []

    monkeypatch.setattr(tiler_generator, "_generate_for_zarr", fake_one)
    monkeypatch.setattr(tiler_generator, "log_memory_usage", lambda *a, **k: None)

    generate_vector_parquet_for_zarrs(api, uuid=None)
    assert calls == [("uuid-a", "a.zarr"), ("uuid-b", "b.zarr")]
