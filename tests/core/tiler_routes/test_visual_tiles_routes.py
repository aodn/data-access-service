"""Visual-tile endpoints: raster tile, bbox render, colormaps, legend."""

from unittest.mock import AsyncMock, patch

from data_access_service.config.http_cache import (
    IMMUTABLE_CACHE_HEADERS,
    REVALIDATE_CACHE_HEADERS,
)

from .conftest import NON_VISUAL_PRODUCT_ID, SEEDED_DATE_ISO, VISUAL_PRODUCT_ID

BASE = "/api/v1/das/tiler/visual_tiles"
_PNG = b"\x89PNG\r\n\x1a\n"


def _tile_url(product=VISUAL_PRODUCT_ID, z=1, x=0, y=0, ext="png", extra=""):
    return f"{BASE}/{product}/{z}/{x}/{y}.{ext}?date={SEEDED_DATE_ISO}{extra}"


# --- GET /{product_id}/{z}/{x}/{y}.{ext} -------------------------------------


def test_tile_ok_png(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_tile",
        return_value=_PNG,
    ):
        r = client.get(_tile_url())
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == IMMUTABLE_CACHE_HEADERS["Cache-Control"]


def test_tile_ok_webp(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_tile",
        return_value=b"RIFF....WEBP",
    ):
        r = client.get(_tile_url(ext="webp"))
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/webp"


def test_tile_unknown_product_404(client):
    r = client.get(_tile_url(product="nonexistent"))
    assert r.status_code == 404


def test_tile_non_visual_product_400(client):
    r = client.get(_tile_url(product=NON_VISUAL_PRODUCT_ID))
    assert r.status_code == 400


def test_tile_invalid_colormap_400(client):
    r = client.get(_tile_url(extra="&colormap=definitely-not-a-colormap"))
    assert r.status_code == 400


def test_tile_zoom_out_of_range_400(client):
    r = client.get(_tile_url(z=99))
    assert r.status_code == 400


def test_tile_xy_out_of_range_400(client):
    # z=1 => valid x/y range is 0..1; 5 is out of range.
    r = client.get(_tile_url(z=1, x=5, y=5))
    assert r.status_code == 400


def test_tile_bad_date_422(client):
    r = client.get(f"{BASE}/{VISUAL_PRODUCT_ID}/1/0/0.png?date=not-a-date")
    assert r.status_code == 422


def test_tile_client_disconnect_499_short_circuits(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.visual_tiles.render_tile"
        ) as render_mock,
        patch(
            "starlette.requests.Request.is_disconnected",
            new=AsyncMock(return_value=True),
        ),
    ):
        r = client.get(_tile_url())
        render_mock.assert_not_called()
    assert r.status_code == 499


# --- GET /{product_id}/bbox.{ext} --------------------------------------------


def test_bbox_ok(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_bbox",
        return_value=_PNG,
    ):
        r = client.get(
            f"{BASE}/{VISUAL_PRODUCT_ID}/bbox.png"
            f"?date={SEEDED_DATE_ISO}&bbox=140,-40,150,-30&crs=EPSG:4326"
        )
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == IMMUTABLE_CACHE_HEADERS["Cache-Control"]


def test_bbox_default_bbox_when_omitted(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_bbox",
        return_value=_PNG,
    ) as render_mock:
        r = client.get(f"{BASE}/{VISUAL_PRODUCT_ID}/bbox.png?date={SEEDED_DATE_ISO}")
    assert r.status_code == 200
    render_mock.assert_called_once()


def test_bbox_bad_bbox_400(client):
    r = client.get(
        f"{BASE}/{VISUAL_PRODUCT_ID}/bbox.png?date={SEEDED_DATE_ISO}&bbox=1,2,3"
    )
    assert r.status_code == 400


def test_bbox_bad_crs_400(client):
    r = client.get(
        f"{BASE}/{VISUAL_PRODUCT_ID}/bbox.png"
        f"?date={SEEDED_DATE_ISO}&bbox=140,-40,150,-30&crs=EPSG:9999"
    )
    assert r.status_code == 400


# --- GET /colormaps and /colormaps/{name}/legend -----------------------------


def test_colormaps_list(client):
    r = client.get(f"{BASE}/colormaps")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"custom", "rio_tiler", "matplotlib"}
    assert r.headers["cache-control"] == REVALIDATE_CACHE_HEADERS["Cache-Control"]


def test_legend_ok(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_legend",
        return_value=_PNG,
    ):
        r = client.get(f"{BASE}/colormaps/viridis/legend")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == IMMUTABLE_CACHE_HEADERS["Cache-Control"]


def test_legend_unknown_colormap_404(client):
    r = client.get(f"{BASE}/colormaps/definitely-not-a-colormap/legend")
    assert r.status_code == 404
