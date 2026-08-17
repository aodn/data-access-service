"""visual_renderer: unit-level tests for reprojection + rendering.

These run the real rio-tiler pipeline against synthetic in-memory datasets,
so they catch antimeridian, rescale, empty-tile, and CRS-conversion edge
cases without needing a real Zarr store.
"""

import io
import threading
import time

import numpy as np
import pytest
import xarray as xr
from PIL import Image

import data_access_service.tiler.services.rendering.visual_tiles as visual_renderer
from data_access_service.tiler.services.product.product import CoastalFill
from data_access_service.tiler.utils.image import TILE_SIZE


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
# _to_scalar_parts: antimeridian + lat/lon-bounds validation
# ---------------------------------------------------------------------------


def test_to_scalar_parts_returns_single_segment_for_typical_grid():
    ds = _scalar_ds()
    parts = visual_renderer._to_scalar_parts(ds, "GSLA")
    assert len(parts) == 1
    assert parts[0].dtype == np.float32


def test_to_scalar_parts_global_grid_wraps_to_negative_lons():
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
    parts = visual_renderer._to_scalar_parts(ds, "v")
    assert len(parts) == 1
    # After wrap, the min lon should be roughly the original (358 - 360) = -2.
    assert parts[0].lon.values.min() < 0


def test_to_scalar_parts_antimeridian_straddle_splits_into_two():
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
    parts = visual_renderer._to_scalar_parts(ds, "GSLA")
    assert len(parts) == 2, "antimeridian straddle should produce 2 segments"
    # Primary contains lons < 180; minor contains negative lons (shifted from > 180).
    assert parts[0].lon.values.max() < 180
    assert parts[1].lon.values.max() < 0


def test_to_scalar_parts_rejects_non_geographic_crs():
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
        visual_renderer._to_scalar_parts(ds, "v")


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


def test_to_scalar_parts_no_coastal_fill_leaves_gap_nan():
    ds = _ds_with_gap()
    parts = visual_renderer._to_scalar_parts(ds, "GSLA")
    assert np.isnan(parts[0].values[7:9, 7:9]).all()


def test_to_scalar_parts_coastal_fill_fills_gap_within_distance():
    # Over open ocean: _ds_with_gap's default box overlaps mainland Australia,
    # where the land cut (also applied here — see _to_scalar_parts) would
    # immediately erase the fill, masking the effect this test checks for.
    ds = _ocean_ds_with_gap()
    parts = visual_renderer._to_scalar_parts(ds, "GSLA", CoastalFill(max_dist_px=4))
    assert not np.isnan(parts[0].values[7:9, 7:9]).any()


def test_to_scalar_parts_coastal_fill_cuts_fill_that_lands_on_land():
    """The reverse of the ocean case above: over land, coastal_fill's fill must
    not survive — this is the land-cut this whole feature is guarding."""
    ds = _ds_with_gap()  # lat(-40,-30)/lon(140,150): mainland southeastern Australia
    parts = visual_renderer._to_scalar_parts(ds, "GSLA", CoastalFill(max_dist_px=4))
    assert np.isnan(parts[0].values[7:9, 7:9]).all()


def test_to_scalar_parts_coastal_fill_respects_max_dist_px():
    ds = _ds_with_gap()
    parts = visual_renderer._to_scalar_parts(ds, "GSLA", CoastalFill(max_dist_px=0))
    assert np.isnan(parts[0].values[7:9, 7:9]).all()


# ---------------------------------------------------------------------------
# _rescale_range
# ---------------------------------------------------------------------------


def test_rescale_range_uses_explicit_value_when_given():
    parts = [_scalar_ds()["GSLA"]]
    assert visual_renderer._rescale_range(parts, (10.0, 20.0)) == (10.0, 20.0)


def test_rescale_range_returns_data_min_max_when_no_arg():
    parts = [_scalar_ds()["GSLA"]]
    lo, hi = visual_renderer._rescale_range(parts, None)
    assert lo == pytest.approx(0.0)
    assert hi == pytest.approx(1.0)


def test_rescale_range_returns_none_for_all_nan_data():
    """Empty/all-NaN data should signal 'nothing to render'."""
    parts = [_scalar_ds(fill="nan")["GSLA"]]
    assert visual_renderer._rescale_range(parts, None) is None


# ---------------------------------------------------------------------------
# render_tile + render_bbox: end-to-end with real rio-tiler pipeline
# ---------------------------------------------------------------------------


def _decode_png(data: bytes) -> np.ndarray:
    return np.array(Image.open(io.BytesIO(data)))


def test_render_tile_returns_png_with_correct_size():
    ds = _scalar_ds()
    # Pick a zoom + tile that overlaps the data bbox roughly.
    out = visual_renderer.render_tile(ds, "GSLA", x=29, y=18, z=5)
    img = _decode_png(out)
    assert img.shape == (TILE_SIZE, TILE_SIZE, 4)


def test_render_tile_outside_extent_returns_empty():
    ds = _scalar_ds()
    # Tile far from the data — fully transparent.
    out = visual_renderer.render_tile(ds, "GSLA", x=0, y=0, z=5)
    img = _decode_png(out)
    assert (img[..., 3] == 0).all()


def test_render_tile_all_nan_returns_empty_tile():
    ds = _scalar_ds(fill="nan")
    out = visual_renderer.render_tile(ds, "GSLA", x=29, y=18, z=5)
    img = _decode_png(out)
    assert (img[..., 3] == 0).all()


def test_render_tile_webp_format():
    ds = _scalar_ds()
    out = visual_renderer.render_tile(ds, "GSLA", x=29, y=18, z=5, fmt="webp")
    assert out.startswith(b"RIFF")  # WebP magic


def test_render_tile_rescale_changes_output():
    """Two tiles rendered with different rescale ranges must NOT be byte-identical."""
    ds = _scalar_ds()
    a = visual_renderer.render_tile(ds, "GSLA", 29, 18, 5, rescale=(0.0, 1.0))
    b = visual_renderer.render_tile(ds, "GSLA", 29, 18, 5, rescale=(0.0, 0.1))
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
        ds, "GSLA", bbox=bbox, width=64, height=64
    )
    with_fill = visual_renderer.render_bbox(
        ds,
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
        ds, "GSLA", bbox=(140.0, -40.0, 150.0, -30.0), width=128, height=128
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
        ds,
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
        ds, "GSLA", bbox=(-100.0, -10.0, -90.0, 0.0), width=64, height=64
    )
    img = _decode_png(out)
    assert (img[..., 3] == 0).all()


def test_render_bbox_animation_produces_apng_with_multiple_frames():
    # Distinct fill patterns so Pillow's APNG encoder doesn't collapse identical
    # adjacent frames into one (it does this for byte-identical frames).
    ds_a = _scalar_ds()
    ds_b = _scalar_ds()
    ds_b["GSLA"].values[:] = ds_b["GSLA"].values[:] * 0.3
    out = visual_renderer.render_bbox_animation(
        [ds_a, ds_b],
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
        [ds, ds],
        "GSLA",
        bbox=(140.0, -40.0, 150.0, -30.0),
        width=32,
        height=32,
        fmt="gif",
        duration_ms=100,
    )
    img = Image.open(io.BytesIO(out))
    assert img.format == "GIF"


# ---------------------------------------------------------------------------
# concurrent stampede protection (always in-process, independent of CACHE_BACKEND)
# ---------------------------------------------------------------------------


def test_concurrent_tiles_at_same_date_share_one_fill_compute(monkeypatch):
    """Two different tiles (z/x/y) for the same (source_path, date, variable,
    coastal_fill) must share one inpaint+land-cut compute when requested
    concurrently, not each redo it independently. This is what `_fill_dedup`
    (services.caching.deduper) protects — see its docstring for why this
    matters even under CACHE_BACKEND=none."""
    ds = _ocean_ds_with_gap()
    calls = 0
    proceed = threading.Event()
    real_inpaint = visual_renderer.inpaint_nearest

    def slow_inpaint(*args, **kwargs):
        nonlocal calls
        calls += 1
        proceed.wait(timeout=2)
        return real_inpaint(*args, **kwargs)

    monkeypatch.setattr(visual_renderer, "inpaint_nearest", slow_inpaint)

    results: list[bytes] = []

    def render(x, y):
        results.append(
            visual_renderer.render_tile(
                ds,
                "GSLA",
                x,
                y,
                5,
                coastal_fill=CoastalFill(max_dist_px=4),
                source_path="s3://test/gap.zarr",
                date="2024-01-01",
            )
        )

    threads = [
        threading.Thread(target=render, args=(29, 18)),
        threading.Thread(target=render, args=(30, 18)),
    ]
    for t in threads:
        t.start()
    time.sleep(0.1)  # let both threads register on the in-flight key
    proceed.set()
    for t in threads:
        t.join(timeout=2)

    assert (
        calls == 1
    ), "expected exactly one inpaint compute; the rest should share it via _fill_dedup"
    assert len(results) == 2


def test_get_filled_values_returns_read_only_array():
    """The cached array is shared across every caller for its key — read-only
    so an accidental downstream mutation fails loudly instead of silently
    corrupting it for every other caller."""
    ds = _ocean_ds_with_gap()
    da = ds["GSLA"].astype(np.float32)
    filled = visual_renderer._get_filled_values(
        "s3://test/gap.zarr",
        "2024-01-01",
        "GSLA",
        CoastalFill(max_dist_px=4),
        da.values,
        da.lon.values,
        da.lat.values,
    )
    assert not filled.flags.writeable
    with pytest.raises(ValueError, match="read-only"):
        filled[0, 0] = 1.0
