"""Parquet-backed visual tiles: catalog + PNG/WebP render."""

from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_access_service.batch.tiler.generator import (
    _timestamp_key,
    preprocess_dataarray,
)
from data_access_service.core.duckdbclient import TilerDuckDBClient
from data_access_service.models.tiler_types import TilerVectorConfig
from data_access_service.tiler.catalog import close_client, refresh_catalog
from data_access_service.tiler.product import get_product
from data_access_service.tiler.render import render_bbox, render_tile
from data_access_service.tiler.utils.dates import iso_timestamp


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


def test_catalog_and_tile_from_parquet(vector_cfg, monkeypatch):
    from data_access_service.batch.tiler.generator import _publish_meta

    da = _grid()
    close_client()
    with TilerDuckDBClient(vector_cfg) as client:
        frag = preprocess_dataarray(
            da,
            uuid="demo-uuid",
            dataset="demo.zarr",
            variable="sst",
            client=client,
            config=vector_cfg,
        )
        dest = client.finalize("demo-uuid")
        _publish_meta(
            client,
            "demo-uuid",
            {
                "uuid": "demo-uuid",
                "parquet": dest,
                "variables": [frag],
                "timestamps": frag["timestamps"],
            },
        )

    cfg_holder = MagicMock()
    cfg_holder.get_tiler_vector_config.return_value = vector_cfg
    monkeypatch.setattr(
        "data_access_service.tiler.catalog.Config.get_config",
        lambda: cfg_holder,
    )
    close_client()
    products = refresh_catalog()
    assert "demo:sst" in products
    product = get_product("demo:sst")
    assert product is not None
    date = iso_timestamp(frag["timestamps"][0])
    png = render_tile(
        product, date, x=0, y=0, z=1, colormap="gray", rescale=None, fmt="png"
    )
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    bbox_png = render_bbox(
        product,
        date,
        150.0,
        -5.0,
        160.0,
        5.0,
        64,
        64,
        "viridis",
        None,
        "png",
    )
    assert bbox_png[:8] == b"\x89PNG\r\n\x1a\n"
    close_client()
