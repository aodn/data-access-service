"""Every renderer gives the same bytes whether its slice is held in memory
(``SparseGrid``, what the tiler read before) or asked of DuckDB
(``ParquetGridSource``, what ``load_slice`` returns now).

The parquet is written the way batch writes it, the in-memory slice is
built from the same rows, and each endpoint's renderer runs over both. Any
difference - a block edge, a range, a masked cell - shows up as a byte.
"""

import os

import duckdb
import morecantile
import numpy as np
import pandas as pd
import pytest

import data_access_service.tiler.services.store.tiler_repository as repo_module
from data_access_service.batch.tiler.parquet_generator import _sparse_rows_for_slice
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    variable_parquet_path,
)
from data_access_service.tiler.services.product.manifest import render_manifest
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
    get_lod_grids,
)
from data_access_service.tiler.services.rendering import data_tiles, visual_tiles
from data_access_service.tiler.services.rendering.masks import ocean_valid_for_coords
from data_access_service.tiler.services.store.parquet_grid_source import value_range
from data_access_service.tiler.services.store.registry import store_registry
from data_access_service.tiler.services.store.slice_loader import load_slice
from data_access_service.tiler.services.store.sparse_grid import (
    SparseGrid,
    SparseSlice,
)
from data_access_service.tiler.services.store.spatial import native_resolution_in_bbox

T0 = "2024-01-15T00:00:00.000000000Z"
T1 = "2024-01-16T00:00:00.000000000Z"
CAT_ATTRS = {"flag_values": [0, 1, 2], "flag_meanings": "a b c"}

# name -> (lat, lon). All but "global" sit inside the ocean mask (lon
# 50-190E, lat 60S-10N). A visual tile only averages once its window is at
# least 2048 cells across (``_steps`` divides by 256 x ``_OVERSAMPLE``), so
# "wide" is past that at low zooms; "region" is big enough for data tiles'
# coarse LODs to. "global" wraps, so its columns are reordered and can't be
# sliced. "crossing" spans 180E.
GRIDS = {
    "wide": (np.linspace(-60, -5, 900), np.linspace(100, 175, 2600)),
    "region": (np.linspace(-60, -5, 1100), np.linspace(100, 175, 1500)),
    "region_desc": (np.linspace(-5, -60, 700), np.linspace(100, 175, 900)),
    "global": (np.linspace(-89.5, 89.5, 180), np.arange(0.5, 360, 1.0)),
    "crossing": (np.linspace(-50, -10, 200), np.linspace(150, 210, 300)),
}
_WEB_MERCATOR = morecantile.tms.get("WebMercatorQuad")


def _values(lat, lon, seed):
    """Smooth field with holes (a coastline's worth of NaN), and a
    categorical field over the same cells."""
    rng = np.random.default_rng(seed)
    la, lo = np.meshgrid(lat, lon, indexing="ij")
    field = np.sin(la / 7.0) + np.cos(lo / 11.0) + rng.normal(0, 0.05, la.shape)
    field = field.astype(np.float32)
    field[rng.random(la.shape) < 0.3] = np.nan
    field[(la > -30) & (la < -25)] = np.nan  # a band of land
    cat = np.where(np.isnan(field), np.nan, np.floor((field + 2.2) % 3))
    return {"sst": field, "u": field * 0.5, "cat": cat.astype(np.float32)}


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    """Write every store's parquet once; the dense values it holds."""
    root = tmp_path_factory.mktemp("tiler")
    dense = {}
    con = duckdb.connect(":memory:")
    for store, (lat, lon) in GRIDS.items():
        for n, ts in enumerate([T0, T1]):
            for v, arr in _values(lat, lon, seed=n).items():
                dense[store, v, ts] = arr
                path = variable_parquet_path(str(root), store, v, ts)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                con.register("f", _sparse_rows_for_slice(arr))
                con.execute(f"COPY f TO '{path}' (FORMAT PARQUET)")
                con.unregister("f")
    con.close()
    return root, dense


@pytest.fixture(autouse=True)
def stores(data, monkeypatch):
    root, _ = data
    monkeypatch.setattr(
        repo_module.Config.get_config(), "get_tiler_root_dir", lambda: str(root)
    )
    store_registry.clear()
    value_range.cache_clear()
    for store, (lat, lon) in GRIDS.items():
        attrs = {"sst": {"units": "C"}, "u": {"units": "m/s"}, "cat": CAT_ATTRS}
        store_registry._publish(
            store,
            TilerParquetMetadata(
                uuid=store,
                dataset=f"{store}.zarr",
                n_i=len(lat),
                n_j=len(lon),
                lat=lat.tolist(),
                lon=lon.tolist(),
                timestamps=[T0, T1],
                variables={
                    v: TilerVariableMetadata(dtype="float32", attrs=a)
                    for v, a in attrs.items()
                },
                generated_at="",
            ),
        )
    yield
    store_registry.clear()
    value_range.cache_clear()


def _both(data, store, variables, ts=T0, ocean_masked=False):
    """The same slice twice: in memory, as the tiler read it before, and
    lazily from the parquet."""
    _, dense = data
    new = load_slice(store, pd.Timestamp(ts.rstrip("Z")), variables, ocean_masked)
    grids = {}
    for v in variables:
        arr = dense[store, v, ts].copy()
        if ocean_masked:
            arr[~ocean_valid_for_coords(new.lon, new.lat)] = np.nan
        i, j = np.nonzero(np.isfinite(arr))
        grids[v] = SparseGrid.from_rows(i, j, arr[i, j], *arr.shape)
    old = SparseSlice(lat=new.lat, lon=new.lon, grids=grids, attrs=new.attrs)
    return old, new


def _tiles(store, zooms, per_zoom=6):
    lat, lon = GRIDS[store]
    west, east = float(lon.min()), min(float(lon.max()), 180.0)
    south, north = float(lat.min()), float(lat.max())
    for z in zooms:
        tiles = list(_WEB_MERCATOR.tiles(west, south, east, north, [z]))
        # Spread the sample over the extent rather than one corner.
        for t in tiles[:: max(1, len(tiles) // per_zoom)][:per_zoom]:
            yield t.x, t.y, z


# --- visual tiles ---


@pytest.mark.parametrize(
    "store, variable, options",
    [
        ("wide", "sst", {}),
        ("wide", "sst", {"rescale": (-1.0, 1.5), "colormap_name": "plasma"}),
        ("wide", "sst", {"coastal_fill": CoastalFill(max_dist_px=3)}),
        ("wide", "cat", {}),
        ("region", "sst", {}),
        ("region_desc", "sst", {"fmt": "webp"}),
        ("global", "sst", {}),
        ("crossing", "sst", {}),
    ],
)
def test_visual_tiles_match(data, store, variable, options):
    old, new = _both(data, store, [variable])
    for x, y, z in _tiles(store, range(0, 8)):
        assert visual_tiles.render_tile(
            new, variable, x, y, z, **options
        ) == visual_tiles.render_tile(old, variable, x, y, z, **options), (x, y, z)


def test_visual_tiles_match_under_the_ocean_mask(data):
    old, new = _both(data, "wide", ["sst"], ocean_masked=True)
    for x, y, z in _tiles("wide", range(0, 7)):
        assert visual_tiles.render_tile(
            new, "sst", x, y, z
        ) == visual_tiles.render_tile(old, "sst", x, y, z), (x, y, z)


# --- bbox ---


@pytest.mark.parametrize(
    "store, bbox, crs, dst_crs, size",
    [
        ("region", (110.0, -50.0, 160.0, -10.0), "EPSG:4326", "EPSG:3857", (256, 256)),
        ("region", (110.0, -50.0, 160.0, -10.0), "EPSG:4326", "EPSG:4326", (800, 600)),
        # The largest window: cell for cell at 2048 wide.
        ("region", (100.0, -60.0, 175.0, -5.0), "EPSG:4326", "EPSG:3857", (2048, 2048)),
        # Wider than 4 x 256, so it averages.
        ("wide", (100.0, -60.0, 175.0, -5.0), "EPSG:4326", "EPSG:3857", (256, 256)),
        ("region", (120.0, -40.0, 125.0, -35.0), "EPSG:4326", "EPSG:3857", None),
        ("region", (1.5e7, -5e6, 1.7e7, -3e6), "EPSG:3857", "EPSG:3857", (512, 512)),
        ("global", (-30.0, -60.0, 60.0, 60.0), "EPSG:4326", "EPSG:3857", (400, 300)),
        (
            "crossing",
            (160.0, -45.0, 180.0, -15.0),
            "EPSG:4326",
            "EPSG:3857",
            (300, 300),
        ),
    ],
)
def test_bbox_matches(data, store, bbox, crs, dst_crs, size):
    old, new = _both(data, store, ["sst"])
    if size is None:
        size = native_resolution_in_bbox(store, bbox)
    args = (bbox, *size)
    options = {"crs": crs, "dst_crs": dst_crs}
    assert visual_tiles.render_bbox(new, "sst", *args, **options) == (
        visual_tiles.render_bbox(old, "sst", *args, **options)
    )


# --- animation ---


def test_animation_matches(data):
    bbox = (110.0, -50.0, 160.0, -10.0)
    frames = {"old": [], "new": []}
    for ts in [T0, T1]:
        old, new = _both(data, "region", ["sst"], ts=ts)
        frames["old"].append(
            visual_tiles.cut_frame(old, "sst", bbox, "EPSG:4326", None, 320, 240)
        )
        frames["new"].append(
            visual_tiles.cut_frame(new, "sst", bbox, "EPSG:4326", None, 320, 240)
        )

    assert visual_tiles.render_bbox_animation(
        frames["new"], "sst", bbox, 320, 240
    ) == visual_tiles.render_bbox_animation(frames["old"], "sst", bbox, 320, 240)


# --- data tiles ---


@pytest.mark.parametrize(
    "store, variable, config, ocean_masked",
    [
        ("region", "sst", DataTileConfig(), False),
        (
            "region",
            "sst",
            DataTileConfig(coastal_fill=CoastalFill(max_dist_px=4)),
            False,
        ),
        ("region", ["u", "sst"], DataTileConfig(), True),
        ("region", "cat", DataTileConfig(), False),
        # Ascending latitude is flipped before resampling.
        ("region_desc", "sst", DataTileConfig(), False),
    ],
)
def test_data_tiles_and_manifest_match(data, store, variable, config, ocean_masked):
    product = Product(
        id="p",
        store=store,
        variable=variable,
        data_tile=config,
        ocean_masked=ocean_masked,
    )
    old, new = _both(data, store, product.variables, ocean_masked=ocean_masked)

    # The manifest's ranges are what the client decodes the tiles with.
    get_lod_grids(product)
    assert render_manifest(product, new) == render_manifest(product, old)

    for lod, (cols, rows) in product.data_tile.lod_grids.items():
        for cy in range(rows):
            for cx in range(cols):
                assert data_tiles.render_tile(
                    product, new, lod, cx, cy
                ) == data_tiles.render_tile(product, old, lod, cx, cy), (
                    lod,
                    cx,
                    cy,
                )


# --- point ---


@pytest.mark.parametrize("ocean_masked", [False, True])
def test_points_match(data, ocean_masked):
    old, new = _both(data, "region", ["sst"], ocean_masked=ocean_masked)
    rng = np.random.default_rng(3)
    for i, j in zip(rng.integers(0, 1100, 40), rng.integers(0, 1500, 40)):
        want = old.grids["sst"].value_at(i, j)
        got = new.grids["sst"].value_at(i, j)
        assert got == want or (np.isnan(got) and np.isnan(want)), (i, j)


# --- the cases above must reach both read paths ---


def test_the_cases_reach_the_aggregate_path(data, monkeypatch):
    """Low zooms and coarse LODs are where block means are read; if the grids
    above shrank past that, the parity tests would pass without ever
    checking them."""
    calls = []
    real = repo_module.TilerParquetRepository.fetch_block_sums

    def spy(self, *args, **kwargs):
        calls.append(args)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(repo_module.TilerParquetRepository, "fetch_block_sums", spy)
    _, wide = _both(data, "wide", ["sst"])
    _, region = _both(data, "region", ["sst"])

    visual_tiles.render_tile(wide, "sst", 0, 0, 0)
    assert calls, "a z=0 visual tile over the wide grid should aggregate"

    calls.clear()
    product = Product(id="p", store="region", variable="sst")
    coarsest = min(get_lod_grids(product))
    data_tiles.render_tile(product, region, coarsest, 0, 0)
    assert calls, "the coarsest data-tile LOD should aggregate"
