"""HTTP caching contract + the data-tile 501 behaviour on the parquet backend."""

from unittest.mock import patch

from data_access_service.config.http_cache import (
    IMMUTABLE_CACHE_HEADERS,
    REVALIDATE_CACHE_HEADERS,
)

from .conftest import SEEDED_DATE_ISO, VISUAL_PRODUCT_ID

DATA_TILES = "/api/v1/das/tiler/data_tiles"
VISUAL_TILES = "/api/v1/das/tiler/visual_tiles"

_REVALIDATE = REVALIDATE_CACHE_HEADERS["Cache-Control"]
_IMMUTABLE = IMMUTABLE_CACHE_HEADERS["Cache-Control"]
_PNG = b"\x89PNG\r\n\x1a\n"


# --- data-tile raster is not served on the parquet backend -------------------


def test_data_tile_png_is_501(client):
    r = client.get(f"{DATA_TILES}/{VISUAL_PRODUCT_ID}/1/0/0.png?date={SEEDED_DATE_ISO}")
    assert r.status_code == 501


# --- REVALIDATE endpoints: short CDN TTL, no ETag ----------------------------


def test_products_is_revalidate(client):
    r = client.get(f"{DATA_TILES}/products")
    assert r.status_code == 200
    assert r.headers["cache-control"] == _REVALIDATE
    assert "etag" not in r.headers


def test_manifest_is_revalidate(client):
    with patch(
        "data_access_service.core.tiler_routes.products.available_dates_iso",
        return_value=[],
    ):
        r = client.get(f"{DATA_TILES}/manifest")
    assert r.status_code == 200
    assert r.headers["cache-control"] == _REVALIDATE
    assert "etag" not in r.headers


def test_colormaps_is_revalidate(client):
    r = client.get(f"{VISUAL_TILES}/colormaps")
    assert r.status_code == 200
    assert r.headers["cache-control"] == _REVALIDATE
    assert "etag" not in r.headers


# --- IMMUTABLE endpoints: 1 year at CDN, no browser caching ------------------


def test_point_is_immutable(client):
    with patch(
        "data_access_service.core.tiler_routes.products.lookup_point",
        return_value=0.1,
    ):
        r = client.get(
            f"{DATA_TILES}/{VISUAL_PRODUCT_ID}/point"
            f"?date={SEEDED_DATE_ISO}&lat=-35&lon=145"
        )
    assert r.status_code == 200
    assert r.headers["cache-control"] == _IMMUTABLE


def test_visual_tile_is_immutable(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_tile",
        return_value=_PNG,
    ):
        r = client.get(
            f"{VISUAL_TILES}/{VISUAL_PRODUCT_ID}/1/0/0.png?date={SEEDED_DATE_ISO}"
        )
    assert r.status_code == 200
    assert r.headers["cache-control"] == _IMMUTABLE


def test_legend_is_immutable(client):
    with patch(
        "data_access_service.core.tiler_routes.visual_tiles.render_legend",
        return_value=_PNG,
    ):
        r = client.get(f"{VISUAL_TILES}/colormaps/viridis/legend")
    assert r.status_code == 200
    assert r.headers["cache-control"] == _IMMUTABLE
