import numpy as np
import xarray as xr

import data_access_service.tiler.services.rendering.data_tiles as data_tiles_module
from data_access_service.tiler.services.product.manifest import render_manifest
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
)
from data_access_service.tiler.services.rendering.data_tiles import render_tile
from data_access_service.tiler.services.rendering.kernels import resample_window
from data_access_service.tiler.services.store.sparse_grid import SparseGrid
from tests.tiler.sparse_helpers import sparse_of


def _make_ds(variables: list[str]) -> xr.Dataset:
    lat = np.linspace(-40, -30, 16)
    lon = np.linspace(140, 155, 16)
    return xr.Dataset(
        {
            v: xr.DataArray(
                np.random.rand(16, 16),
                dims=["lat", "lon"],
                coords={"lat": lat, "lon": lon},
            )
            for v in variables
        }
    )


SCALAR_PRODUCT = Product(
    id="test_scalar",
    store="",
    variable="sst",
    data_tile=DataTileConfig(lod_grids={1: (1, 1)}, chunk_px=(8, 8), padding=0),
)

UV_PRODUCT = Product(
    id="test_uv",
    store="",
    variable=["u", "v"],
    data_tile=DataTileConfig(lod_grids={1: (1, 1)}, chunk_px=(8, 8), padding=0),
)


def test_render_tile_scalar_is_valid_png():
    ds = _make_ds(["sst"])
    png = render_tile(SCALAR_PRODUCT, lambda: sparse_of(ds), 1, 0, 0)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_render_tile_uv_is_valid_png():
    ds = _make_ds(["u", "v"])
    png = render_tile(UV_PRODUCT, lambda: sparse_of(ds), 1, 0, 0)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_render_manifest_scalar_shape():
    manifest = render_manifest(SCALAR_PRODUCT, sparse_of(_make_ds(["sst"])))
    assert set(manifest) >= {"bounds", "valueRange", "lods"}
    assert len(manifest["valueRange"]) == 2
    bounds = manifest["bounds"]
    assert bounds["lonMin"] < bounds["lonMax"]
    assert bounds["latMin"] < bounds["latMax"]


def test_render_manifest_uv_shape():
    manifest = render_manifest(UV_PRODUCT, sparse_of(_make_ds(["u", "v"])))
    assert set(manifest) >= {"bounds", "uRange", "vRange", "lods"}
    assert "valueRange" not in manifest


def _make_categorical_ds(
    flag_meanings: str | None = "none moderate strong severe extreme",
):
    ds = _make_ds(["cat"])
    ds["cat"].attrs["flag_values"] = [0, 1, 2, 3, 4]
    if flag_meanings is not None:
        ds["cat"].attrs["flag_meanings"] = flag_meanings
    return ds


CATEGORICAL_PRODUCT = Product(
    id="test_cat",
    store="",
    variable="cat",
    data_tile=DataTileConfig(lod_grids={1: (1, 1)}, chunk_px=(8, 8), padding=0),
)


def test_render_manifest_categorical_includes_flag_values_and_meanings():
    manifest = render_manifest(CATEGORICAL_PRODUCT, sparse_of(_make_categorical_ds()))
    assert manifest["flagValues"] == [0, 1, 2, 3, 4]
    assert manifest["flagMeanings"] == [
        "none",
        "moderate",
        "strong",
        "severe",
        "extreme",
    ]
    # The scalar value range is still emitted alongside the categorical fields.
    assert len(manifest["valueRange"]) == 2


def test_render_manifest_continuous_has_no_flag_fields():
    manifest = render_manifest(SCALAR_PRODUCT, sparse_of(_make_ds(["sst"])))
    assert "flagValues" not in manifest
    assert "flagMeanings" not in manifest


def test_render_manifest_categorical_omits_misaligned_meanings():
    # 2 labels for 5 values → flag_meanings is dropped, flagValues still present.
    ds = _make_categorical_ds(flag_meanings="only two")
    manifest = render_manifest(CATEGORICAL_PRODUCT, sparse_of(ds))
    assert manifest["flagValues"] == [0, 1, 2, 3, 4]
    assert "flagMeanings" not in manifest


# --- resampling: categorical → nearest, continuous → bilinear ---------------


def _two_by_two_ds(variable: str, flag_values: list[int] | None) -> xr.Dataset:
    # Sharp 0/4 checkerboard so blended values (1/2/3) are unmistakable if they appear.
    arr = np.array([[0.0, 4.0], [4.0, 0.0]], dtype="float32")
    da = xr.DataArray(
        arr, dims=["lat", "lon"], coords={"lat": [1.0, 0.0], "lon": [0.0, 1.0]}
    )
    if flag_values is not None:
        da.attrs["flag_values"] = flag_values
    return xr.Dataset({variable: da})


def _resample_all(ds: xr.Dataset, variable: str, nearest: bool) -> np.ndarray:
    grid = sparse_of(ds).grids[variable]
    return resample_window(grid, 8, 8, (0, 8), (0, 8), flip=False, nearest=nearest)


def test_resample_categorical_uses_nearest_no_blended_codes():
    ds = _two_by_two_ds("cat", flag_values=[0, 4])
    out = _resample_all(ds, "cat", nearest=True)
    # Nearest must reproduce only the source codes — never an interpolated 1/2/3.
    assert set(np.unique(out)).issubset({0.0, 4.0})


def test_resample_continuous_uses_bilinear_blends():
    ds = _two_by_two_ds("cont", flag_values=None)
    out = _resample_all(ds, "cont", nearest=False)
    # Bilinear must produce intermediate values absent from the source set.
    assert not set(np.unique(out)).issubset({0.0, 4.0})


def test_render_tile_categorical_is_valid_png():
    # End-to-end data-tile render of a categorical product must not crash and
    # must produce a valid PNG (resample is nearest under the hood).
    png = render_tile(
        CATEGORICAL_PRODUCT, lambda: sparse_of(_make_categorical_ds()), 1, 0, 0
    )
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


# --- a tile computes only its own window ---------------------------------


def _gappy_ds(n_lat: int, n_lon: int, seed: int = 0, south_first=False) -> xr.Dataset:
    rng = np.random.default_rng(seed)
    arr = rng.random((n_lat, n_lon)).astype(np.float32)
    arr[rng.random(arr.shape) < 0.3] = np.nan
    lat = np.linspace(-40, -30, n_lat)
    lat = lat if south_first else lat[::-1]
    return xr.Dataset(
        {"sst": (("lat", "lon"), arr)},
        coords={"lat": lat, "lon": np.linspace(140, 155, n_lon)},
    )


def test_resample_window_matches_the_same_window_of_the_whole_grid():
    for south_first in (False, True):
        grid = sparse_of(_gappy_ds(37, 53, south_first=south_first)).grids["sst"]
        for nearest in (False, True):
            whole = resample_window(
                grid, 90, 120, (0, 90), (0, 120), flip=south_first, nearest=nearest
            )
            part = resample_window(
                grid, 90, 120, (17, 41), (60, 97), flip=south_first, nearest=nearest
            )
            np.testing.assert_array_equal(part, whole[17:41, 60:97])


def _tiled_product(coastal_fill=None) -> Product:
    return Product(
        id="tiled",
        store="",
        variable="sst",
        data_tile=DataTileConfig(
            lod_grids={1: (3, 2)},
            chunk_px=(8, 6),
            padding=1,
            coastal_fill=coastal_fill,
        ),
    )


def test_tiles_are_the_windows_of_one_whole_grid_render(monkeypatch):
    """Each tile, padding included, equals cutting it out of the whole LOD
    grid computed at once (so there are no seams between tiles)."""
    sparse = sparse_of(_gappy_ds(20, 30))
    for fill in (None, CoastalFill(max_dist_px=2)):
        product = _tiled_product(fill)
        # No land in the test region for the land cut to hide differences.
        monkeypatch.setattr(
            data_tiles_module,
            "land_mask_for_grid",
            lambda *a, rows=slice(None), cols=slice(None): np.zeros(
                (a[5], a[4]), dtype=bool
            )[rows, cols],
        )
        whole, ocean = data_tiles_module._compute_window(
            product, sparse, 1, (0, 12), (0, 24)
        )
        padded = np.pad(whole[0], 1, mode="edge")
        padded_ocean = np.pad(ocean, 1, mode="edge")
        for cy in range(2):
            for cx in range(3):
                png = render_tile(product, lambda: sparse, 1, cx, cy)
                img = _decode(png)
                window = (slice(cy * 6, cy * 6 + 8), slice(cx * 8, cx * 8 + 10))
                value = (
                    (img[..., 0].astype(np.uint32) << 16)
                    | (img[..., 1].astype(np.uint32) << 8)
                    | img[..., 2]
                )
                mask = padded_ocean[window] == 1
                np.testing.assert_array_equal(img[..., 3] > 0, mask)
                np.testing.assert_array_equal(value[mask], padded[window][mask])


def _decode(png: bytes) -> np.ndarray:
    import io

    from PIL import Image

    return np.asarray(Image.open(io.BytesIO(png)).convert("RGBA"))


def test_a_coarse_lod_never_builds_the_source_at_full_resolution(monkeypatch):
    sparse = sparse_of(_gappy_ds(400, 600))
    gathered, aggregated = [], []
    real_gather, real_aggregate = SparseGrid.gather, SparseGrid.aggregate_blocks

    def gather(self, rows, cols):
        gathered.append((len(rows), len(cols)))
        return real_gather(self, rows, cols)

    def aggregate_blocks(self, row_edges, col_edges):
        aggregated.append((len(row_edges) - 1, len(col_edges) - 1))
        return real_aggregate(self, row_edges, col_edges)

    monkeypatch.setattr(SparseGrid, "gather", gather)
    monkeypatch.setattr(SparseGrid, "aggregate_blocks", aggregate_blocks)

    render_tile(SCALAR_PRODUCT, lambda: sparse, 1, 0, 0)

    # Aggregating returns one value per output pixel; sampling reads a couple
    # of source cells per pixel. For an 8x8 tile neither approaches 400x600.
    assert gathered or aggregated
    assert all(r <= 16 and c <= 16 for r, c in gathered)
    assert all(r <= 16 and c <= 16 for r, c in aggregated)


# --- a coarse LOD averages the cells it covers ------------------------------


def _coarse(grid, th, tw, rows, cols, flip):
    return resample_window(grid, th, tw, rows, cols, flip=flip, nearest=False)


def test_a_coarse_lod_tile_matches_that_window_of_the_whole_lod():
    """Blocks must not move with the window asked for, or stitched tiles
    would disagree along their shared edge."""
    for south_first in (False, True):
        grid = sparse_of(_gappy_ds(600, 800, south_first=south_first)).grids["sst"]
        th, tw = 80, 100  # ~7.6 source cells per output pixel
        whole = _coarse(grid, th, tw, (0, th), (0, tw), south_first)
        part = _coarse(grid, th, tw, (24, 56), (30, 70), south_first)
        np.testing.assert_array_equal(part, whole[24:56, 30:70])


def test_a_coarse_lod_still_runs_north_to_south():
    n_lat, n_lon = 600, 800
    for south_first in (False, True):
        # The value of every cell is its own source row.
        arr = np.repeat(np.arange(n_lat, dtype=np.float32)[:, None], n_lon, axis=1)
        lat = np.linspace(-40, -30, n_lat)
        ds = xr.Dataset(
            {"sst": (("lat", "lon"), arr)},
            coords={
                "lat": lat if south_first else lat[::-1],
                "lon": np.linspace(140, 155, n_lon),
            },
        )
        out = _coarse(
            sparse_of(ds).grids["sst"], 80, 100, (0, 80), (0, 100), south_first
        )
        # Ascending lat means the northernmost data sits at the end of the array.
        northmost = n_lat - 1 if south_first else 0
        assert abs(out[0, 0] - northmost) < 12
        assert abs(out[-1, 0] - (n_lat - 1 - northmost)) < 12


def test_a_coarse_lod_keeps_cells_that_sampling_would_step_over():
    from data_access_service.tiler.services.rendering import kernels

    grid = sparse_of(_gappy_ds(600, 800, seed=7)).grids["sst"]
    th, tw = 80, 100
    averaged = _coarse(grid, th, tw, (0, th), (0, tw), False)
    saved = kernels._AGGREGATE_PITCH
    kernels._AGGREGATE_PITCH = 1e9  # force the sampling path
    try:
        sampled = _coarse(grid, th, tw, (0, th), (0, tw), False)
    finally:
        kernels._AGGREGATE_PITCH = saved
    # Every pixel sampling found, averaging found too, and then some.
    assert np.isfinite(averaged).sum() > np.isfinite(sampled).sum()
    assert not (np.isfinite(sampled) & ~np.isfinite(averaged)).any()


def test_near_the_source_resolution_nothing_is_averaged():
    from data_access_service.tiler.services.rendering import kernels

    grid = sparse_of(_gappy_ds(90, 120)).grids["sst"]
    # 90 rows into 80 output pixels: barely more than one cell each.
    assert (
        kernels.aggregate_window(grid, 80, 100, (0, 80), (0, 100), flip=False) is None
    )
