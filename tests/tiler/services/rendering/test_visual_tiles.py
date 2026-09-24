"""visual_renderer: unit-level tests for reprojection + rendering.

These run the real rio-tiler pipeline against synthetic in-memory datasets,
so they catch antimeridian, rescale, empty-tile, and CRS-conversion edge
cases without needing a real Zarr store.
"""

import io

import numpy as np
import pytest
import xarray as xr
from PIL import Image

import data_access_service.tiler.services.rendering.visual_tiles as visual_renderer
from data_access_service.tiler.services.product.product import CoastalFill
from data_access_service.tiler.services.store.sparse_grid import SparseGrid
from data_access_service.tiler.utils.image import TILE_SIZE
from tests.tiler.sparse_helpers import sparse_of

_WORLD = (-180.0, -90.0, 180.0, 90.0)


def _parts(ds: xr.Dataset, var: str, coastal_fill=None) -> list[xr.DataArray]:
    """Every part of the grid (a bbox over the whole world)."""
    return visual_renderer._parts_in_bbox(sparse_of(ds), var, _WORLD, coastal_fill)


def _scalar_ds(
    lat=(-40.0, -30.0), lon=(140.0, 150.0), size=16, var="GSLA", fill="ramp"
) -> xr.Dataset:
    """Build a simple lat/lon scalar dataset in EPSG:4326."""
    lat_arr = np.linspace(lat[0], lat[1], size)
    lon_arr = np.linspace(lon[0], lon[1], size)
    if fill == "ramp":
        data = np.tile(np.linspace(0.0, 1.0, size), (size, 1))
    elif fill == "nan":
        data = np.full((size, size), np.nan)
    else:
        data = np.zeros((size, size))
    return xr.Dataset(
        {
            var: xr.DataArray(
                data, dims=["lat", "lon"], coords={"lat": lat_arr, "lon": lon_arr}
            )
        }
    )


# ---------------------------------------------------------------------------
# _parts_in_bbox: antimeridian + lat/lon-bounds validation
# ---------------------------------------------------------------------------


def test_parts_returns_single_segment_for_typical_grid():
    ds = _scalar_ds()
    parts = _parts(ds, "GSLA")
    assert len(parts) == 1
    assert parts[0].dtype == np.float32


def test_parts_global_grid_wraps_to_negative_lons():
    """Grid spanning 0–360 should wrap to −180..180 contiguous (single segment)."""
    lon = np.linspace(0.0, 358.0, 180)  # 2° spacing — contiguous after wrap
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                np.zeros((10, 180)),
                dims=["lat", "lon"],
                coords={"lat": np.linspace(-45, 45, 10), "lon": lon},
            )
        }
    )
    parts = _parts(ds, "v")
    assert len(parts) == 1
    # After wrap, the min lon should be roughly the original (358 - 360) = -2.
    assert parts[0].lon.values.min() < 0


def test_parts_antimeridian_straddle_splits_into_two():
    """GSLA-style 57°–185°E grid must split into a primary + minor segment."""
    lon = np.linspace(57.0, 185.0, 128)
    ds = xr.Dataset(
        {
            "GSLA": xr.DataArray(
                np.zeros((10, 128)),
                dims=["lat", "lon"],
                coords={"lat": np.linspace(-45, 0, 10), "lon": lon},
            )
        }
    )
    parts = _parts(ds, "GSLA")
    assert len(parts) == 2, "antimeridian straddle should produce 2 segments"
    # Primary contains lons < 180; minor contains negative lons (shifted from > 180).
    assert parts[0].lon.values.max() < 180
    assert parts[1].lon.values.max() < 0


def test_parts_rejects_non_geographic_crs():
    """Out-of-range lat/lon should raise — guards against accidentally feeding
    a projected dataset."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                np.zeros((4, 4)),
                dims=["lat", "lon"],
                coords={"lat": [200, 201, 202, 203], "lon": [0, 1, 2, 3]},
            )
        }
    )
    with pytest.raises(ValueError, match="EPSG:4326"):
        _parts(ds, "v")


def _ds_with_gap() -> xr.Dataset:
    """All-valid ramp with a small NaN hole in the middle, for coastal-fill tests."""
    ds = _scalar_ds(size=16)
    ds["GSLA"].values[7:9, 7:9] = np.nan
    return ds


def _ocean_ds_with_gap() -> xr.Dataset:
    """Same as _ds_with_gap, but positioned over open Southern Ocean (south of
    Tasmania) so the real land mask doesn't zero out the whole test region —
    _ds_with_gap's default lon/lat box overlaps mainland southeastern Australia."""
    ds = _scalar_ds(lat=(-45.0, -40.0), lon=(150.0, 160.0), size=16)
    ds["GSLA"].values[7:9, 7:9] = np.nan
    return ds


def test_parts_no_coastal_fill_leaves_gap_nan():
    ds = _ds_with_gap()
    parts = _parts(ds, "GSLA")
    assert np.isnan(parts[0].values[7:9, 7:9]).all()


def test_parts_coastal_fill_fills_gap_within_distance():
    # Over open ocean: _ds_with_gap's default box overlaps mainland Australia,
    # where the land cut (also applied here — see _parts_in_bbox) would
    # immediately erase the fill, masking the effect this test checks for.
    ds = _ocean_ds_with_gap()
    parts = _parts(ds, "GSLA", CoastalFill(max_dist_px=4))
    assert not np.isnan(parts[0].values[7:9, 7:9]).any()


def test_parts_coastal_fill_cuts_fill_that_lands_on_land():
    """The reverse of the ocean case above: over land, coastal_fill's fill must
    not survive — this is the land-cut this whole feature is guarding."""
    ds = _ds_with_gap()  # lat(-40,-30)/lon(140,150): mainland southeastern Australia
    parts = _parts(ds, "GSLA", CoastalFill(max_dist_px=4))
    assert np.isnan(parts[0].values[7:9, 7:9]).all()


def test_parts_coastal_fill_respects_max_dist_px():
    ds = _ds_with_gap()
    parts = _parts(ds, "GSLA", CoastalFill(max_dist_px=0))
    assert np.isnan(parts[0].values[7:9, 7:9]).all()


# ---------------------------------------------------------------------------
# _data_range
# ---------------------------------------------------------------------------


def test_data_range_is_the_min_max_of_the_data():
    sparse = sparse_of(_scalar_ds())
    lo, hi = visual_renderer._data_range([visual_renderer._grid_range(sparse, "GSLA")])
    assert lo == pytest.approx(0.0)
    assert hi == pytest.approx(1.0)


def test_data_range_spans_every_frame():
    assert visual_renderer._data_range([(0.0, 1.0), (5.0, 6.0)]) == (0.0, 6.0)


def test_data_range_skips_empty_frames():
    assert visual_renderer._data_range([(np.nan, np.nan), (2.0, 3.0)]) == (2.0, 3.0)


def test_data_range_returns_none_for_all_nan_data():
    """Empty/all-NaN data should signal 'nothing to render'."""
    sparse = sparse_of(_scalar_ds(fill="nan"))
    range_ = visual_renderer._grid_range(sparse, "GSLA")
    assert visual_renderer._data_range([range_]) is None


# ---------------------------------------------------------------------------
# render_tile + render_bbox: end-to-end with real rio-tiler pipeline
# ---------------------------------------------------------------------------


def _decode_png(data: bytes) -> np.ndarray:
    return np.array(Image.open(io.BytesIO(data)))


def test_render_tile_returns_png_with_correct_size():
    ds = _scalar_ds()
    # Pick a zoom + tile that overlaps the data bbox roughly.
    out = visual_renderer.render_tile(sparse_of(ds), "GSLA", x=29, y=18, z=5)
    img = _decode_png(out)
    assert img.shape == (TILE_SIZE, TILE_SIZE, 4)


def test_render_tile_outside_extent_returns_empty():
    ds = _scalar_ds()
    # Tile far from the data — fully transparent.
    out = visual_renderer.render_tile(sparse_of(ds), "GSLA", x=0, y=0, z=5)
    img = _decode_png(out)
    assert (img[..., 3] == 0).all()


def test_render_tile_all_nan_returns_empty_tile():
    ds = _scalar_ds(fill="nan")
    out = visual_renderer.render_tile(sparse_of(ds), "GSLA", x=29, y=18, z=5)
    img = _decode_png(out)
    assert (img[..., 3] == 0).all()


def test_render_tile_webp_format():
    ds = _scalar_ds()
    out = visual_renderer.render_tile(
        sparse_of(ds), "GSLA", x=29, y=18, z=5, fmt="webp"
    )
    assert out.startswith(b"RIFF")  # WebP magic


def test_render_tile_rescale_changes_output():
    """Two tiles rendered with different rescale ranges must NOT be byte-identical."""
    ds = _scalar_ds()
    a = visual_renderer.render_tile(
        sparse_of(ds), "GSLA", 29, 18, 5, rescale=(0.0, 1.0)
    )
    b = visual_renderer.render_tile(
        sparse_of(ds), "GSLA", 29, 18, 5, rescale=(0.0, 0.1)
    )
    assert a != b


def test_render_bbox_coastal_fill_increases_opaque_pixel_count():
    """A NaN gap leaves transparent pixels; coastal_fill should make more of them opaque.

    Uses render_bbox (not render_tile) tightly cropped over the gap — at a
    Web Mercator tile's coarser zoom the 2-pixel native gap is too small
    relative to the tile to reliably show up in the resampled output.
    """
    ds = _ocean_ds_with_gap()
    bbox = (154.0, -43.5, 156.0, -41.5)  # brackets the gap in _ocean_ds_with_gap
    without_fill = visual_renderer.render_bbox(
        sparse_of(ds), "GSLA", bbox=bbox, width=64, height=64
    )
    with_fill = visual_renderer.render_bbox(
        sparse_of(ds),
        "GSLA",
        bbox=bbox,
        width=64,
        height=64,
        coastal_fill=CoastalFill(max_dist_px=4),
    )
    opaque_without = (_decode_png(without_fill)[..., 3] > 0).sum()
    opaque_with = (_decode_png(with_fill)[..., 3] > 0).sum()
    assert opaque_with > opaque_without


def test_render_bbox_in_wgs84():
    ds = _scalar_ds()
    out = visual_renderer.render_bbox(
        sparse_of(ds), "GSLA", bbox=(140.0, -40.0, 150.0, -30.0), width=128, height=128
    )
    img = _decode_png(out)
    assert img.shape == (128, 128, 4)
    # Pixels should mostly be opaque since the bbox sits over the data.
    assert (img[..., 3] > 0).sum() > 0


def test_render_bbox_in_mercator():
    """Same data, mercator bbox transformed — should still produce a non-empty image."""
    ds = _scalar_ds()
    # Roughly cover the data region in EPSG:3857.
    out = visual_renderer.render_bbox(
        sparse_of(ds),
        "GSLA",
        bbox=(15_580_000.0, -5_000_000.0, 16_700_000.0, -3_500_000.0),
        width=64,
        height=64,
        crs="EPSG:3857",
    )
    img = _decode_png(out)
    assert img.shape == (64, 64, 4)


def test_render_bbox_outside_data_returns_empty():
    ds = _scalar_ds()
    out = visual_renderer.render_bbox(
        sparse_of(ds), "GSLA", bbox=(-100.0, -10.0, -90.0, 0.0), width=64, height=64
    )
    img = _decode_png(out)
    assert (img[..., 3] == 0).all()


_ANIMATION_BBOX = (140.0, -40.0, 150.0, -30.0)


def _frame(ds: xr.Dataset) -> visual_renderer.BboxFrame:
    return visual_renderer.cut_frame(sparse_of(ds), "GSLA", _ANIMATION_BBOX)


def test_cut_frame_keeps_the_bbox_and_the_whole_slice_range():
    ds = _scalar_ds(lat=(-40.0, -30.0), lon=(140.0, 160.0), size=64)
    frame = visual_renderer.cut_frame(
        sparse_of(ds), "GSLA", (140.0, -40.0, 145.0, -35.0)
    )
    (part,) = frame.parts
    # Only the bbox (plus the margin), not the whole 64x64 grid.
    assert part.shape[0] < 64 and part.shape[1] < 64
    assert float(part.lon.max()) < 147.0
    # The range still covers the whole slice, so frames share one colour scale.
    assert (frame.vmin, frame.vmax) == (pytest.approx(0.0), pytest.approx(1.0))


def test_render_bbox_animation_produces_apng_with_multiple_frames():
    # Distinct fill patterns so Pillow's APNG encoder doesn't collapse identical
    # adjacent frames into one (it does this for byte-identical frames).
    ds_a = _scalar_ds()
    ds_b = _scalar_ds()
    ds_b["GSLA"].values[:] = ds_b["GSLA"].values[:] * 0.3
    out = visual_renderer.render_bbox_animation(
        [_frame(ds_a), _frame(ds_b)],
        "GSLA",
        bbox=(140.0, -40.0, 150.0, -30.0),
        width=64,
        height=64,
        fmt="apng",
        duration_ms=100,
    )
    img = Image.open(io.BytesIO(out))
    assert img.format == "PNG"
    assert getattr(img, "n_frames", 1) == 2


def test_render_bbox_animation_empty_dataset_list_raises():
    with pytest.raises(ValueError):
        visual_renderer.render_bbox_animation(
            [], "GSLA", bbox=(140.0, -40.0, 150.0, -30.0), width=32, height=32
        )


def test_render_bbox_animation_all_nan_does_not_error():
    """All-NaN frames must still produce a valid image. Both GIF and APNG encoders
    collapse adjacent byte-identical frames, so we can't assert n_frames > 1 — we
    just need the call to succeed and produce a valid image."""
    ds = _scalar_ds(fill="nan")
    out = visual_renderer.render_bbox_animation(
        [_frame(ds), _frame(ds)],
        "GSLA",
        bbox=(140.0, -40.0, 150.0, -30.0),
        width=32,
        height=32,
        fmt="gif",
        duration_ms=100,
    )
    img = Image.open(io.BytesIO(out))
    assert img.format == "GIF"


# a tile builds only its window, with the same output
# ---------------------------------------------------------------------------


def _gappy_ocean_ds(lon=(150.0, 160.0), size=64) -> xr.Dataset:
    ds = _scalar_ds(lat=(-46.0, -40.0), lon=lon, size=size)
    rng = np.random.default_rng(0)
    ds["GSLA"].values[rng.random((size, size)) < 0.2] = np.nan
    return ds


@pytest.mark.parametrize("coastal_fill", [None, CoastalFill(max_dist_px=3)])
@pytest.mark.parametrize("lon", [(150.0, 160.0), (175.0, 185.0)])
def test_windowed_tiles_match_whole_grid_tiles(monkeypatch, coastal_fill, lon):
    """Same bytes as building the whole grid (a margin bigger than the grid),
    including a grid split at 180°E."""
    sparse = sparse_of(_gappy_ocean_ds(lon))
    tiles = [(z, *_tile_at(z, lon[0] + 2.0, -43.0)) for z in (4, 6, 8)]
    tiles += [(8, *_tile_at(8, 179.99, -43.0)), (8, *_tile_at(8, -179.99, -43.0))]

    def render_all():
        return [
            visual_renderer.render_tile(
                sparse, "GSLA", x, y, z, coastal_fill=coastal_fill
            )
            for z, x, y in tiles
        ]

    windowed = render_all()
    monkeypatch.setattr(visual_renderer, "_MARGIN_PX", 10**6)
    whole = render_all()

    assert windowed == whole


def _tile_at(z: int, lon: float, lat: float) -> tuple[int, int]:
    t = visual_renderer._WEB_MERCATOR.tile(lon, lat, z)
    return t.x, t.y


def test_a_high_zoom_tile_builds_only_a_small_window(monkeypatch):
    ds = _scalar_ds(lat=(-46.0, -40.0), lon=(150.0, 160.0), size=512)
    shapes = []
    real_gather = SparseGrid.gather

    def gather(self, rows, cols):
        shapes.append((len(rows), len(cols)))
        return real_gather(self, rows, cols)

    monkeypatch.setattr(SparseGrid, "gather", gather)
    x, y = _tile_at(10, 155.0, -43.0)

    visual_renderer.render_tile(sparse_of(ds), "GSLA", x, y, 10)

    assert shapes and all(r < 64 and c < 64 for r, c in shapes)


def _big_ds(n_lat=2600, n_lon=2600, var="GSLA"):
    """A grid far coarser than a tile, so the window has to be aggregated."""
    lat = np.linspace(-10.0, -50.0, n_lat)
    lon = np.linspace(100.0, 160.0, n_lon)
    field = (
        np.linspace(0.0, 1.0, n_lat)[:, None] + np.linspace(0.0, 1.0, n_lon)[None, :]
    ).astype(np.float32)
    return xr.Dataset({var: (("lat", "lon"), field)}, coords={"lat": lat, "lon": lon})


def _window(ds, var, out_width=TILE_SIZE, out_height=TILE_SIZE):
    return visual_renderer._parts_in_bbox(
        sparse_of(ds), var, _WORLD, None, out_width, out_height
    )[0]


def test_a_window_near_the_output_resolution_is_read_cell_for_cell():
    ds = _scalar_ds(size=16)
    part = _window(ds, "GSLA")
    assert part.shape == (16, 16)


def test_a_much_coarser_window_comes_back_as_block_means():
    ds = _big_ds()
    part = _window(ds, "GSLA")
    target = TILE_SIZE * visual_renderer._AVG_OVERSAMPLE
    assert part.shape == (target, target)
    # Block means of a smooth ramp stay inside the original range.
    assert np.nanmin(part.values) >= 0.0
    assert np.nanmax(part.values) <= 2.0


def test_aggregated_window_follows_a_non_square_output():
    """Regression: width and height were swapped on the way to aggregate()."""
    ds = _big_ds()
    part = _window(ds, "GSLA", out_width=512, out_height=128)
    avg = visual_renderer._AVG_OVERSAMPLE
    assert part.shape == (128 * avg, 512 * avg)


def test_aggregated_window_keeps_its_coordinates_lined_up():
    ds = _big_ds()
    part = _window(ds, "GSLA")
    assert part.shape == (len(part.coords["lat"]), len(part.coords["lon"]))
    lat, lon = part.coords["lat"].values, part.coords["lon"].values
    assert lat[0] > lat[-1]  # still north to south
    assert np.all(np.diff(lon) > 0)
    assert -50.0 <= lat.min() and lat.max() <= -10.0
    assert 100.0 <= lon.min() and lon.max() <= 160.0


def test_a_tile_window_never_reaches_the_read_cap():
    """Tiles keep the steps they had before the cap existed."""
    for n in range(1, 40000, 97):
        rs, cs = visual_renderer._steps(n, n, TILE_SIZE, TILE_SIZE)
        old = max(1, n // (TILE_SIZE * visual_renderer._OVERSAMPLE))
        assert (rs, cs) == (old, old)


def test_a_large_output_reads_about_one_cell_per_pixel():
    rs, cs = visual_renderer._steps(9601, 13601, 2048, 2048)
    assert (9601 // rs) * (13601 // cs) <= visual_renderer._MAX_READ_CELLS
    assert 9601 // rs >= 1900 and 13601 // cs >= 1900


def test_a_capped_bbox_still_has_a_block_per_output_pixel():
    n_i, n_j = 9601, 13601  # snpp
    rs, cs = visual_renderer._steps(n_i, n_j, 2048, 2048)
    rows = visual_renderer._block_count(n_i, 2048, rs)
    cols = visual_renderer._block_count(n_j, 2048, cs)
    assert (rows, cols) == (2048, 2048)
    assert rows * cols <= visual_renderer._MAX_READ_CELLS


def test_a_narrow_axis_never_gets_more_blocks_than_cells():
    assert visual_renderer._block_count(1000, 2048, 1) == 1000


def test_a_tile_keeps_two_blocks_per_pixel():
    for n in (1024, 5000, 40000):
        step = visual_renderer._steps(n, n, TILE_SIZE, TILE_SIZE)[0]
        assert visual_renderer._block_count(n, TILE_SIZE, step) == 2 * TILE_SIZE


def test_a_large_bbox_is_averaged_within_the_read_cap():
    ds = _big_ds()  # 2600 x 2600: over the cap, under 4x a 2048 output
    part = _window(ds, "GSLA", out_width=2048, out_height=2048)
    assert part.size <= visual_renderer._MAX_READ_CELLS
    assert np.nanmin(part.values) >= 0.0
    assert np.nanmax(part.values) <= 2.0


def test_a_categorical_variable_is_sampled_not_averaged():
    ds = _big_ds(var="MCS_category")
    # The ramp runs 0..2, so flooring it gives exactly the three codes.
    codes = np.floor(ds["MCS_category"].values).astype(np.float32)
    ds["MCS_category"] = (("lat", "lon"), codes)
    ds["MCS_category"].attrs["flag_values"] = [0, 1, 2]
    ds["MCS_category"].attrs["flag_meanings"] = "none mild severe"
    part = _window(ds, "MCS_category")
    present = np.unique(part.values[np.isfinite(part.values)])
    assert set(present) <= {0.0, 1.0, 2.0}, f"averaging invented codes: {present}"
