import json
from unittest.mock import patch

import numpy as np
import xarray as xr

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import Product


def test_list_products_includes_coastal_fill_only_when_present(
    client, tmp_path, monkeypatch
):
    cfg = tmp_path / "products.json"
    cfg.write_text(
        json.dumps(
            [
                {
                    "id": "sparse",
                    "source_path": "s3://b/x.zarr",
                    "variable": "GSLA",
                    "coastal_fill": {"max_dist_px": 4},
                },
                {"id": "plain", "source_path": "s3://b/y.zarr", "variable": "V"},
            ]
        )
    )
    monkeypatch.setattr(registry, "_config_path", cfg)

    r = client.get("/api/v1/das/tiler/data_tiles/products")
    assert r.status_code == 200
    by_id = {p["id"]: p for p in r.json()}
    assert by_id["sparse"]["coastal_fill"] == {"max_dist_px": 4}
    assert "coastal_fill" not in by_id["plain"]  # omitted when not set


_FAKE_PRODUCTS = {
    "product_a": Product(
        id="product_a", source_path="s3://bucket/a.zarr", variable="VAR"
    ),
}

_LOD_GRIDS = {1: (1, 1)}


def _make_ds() -> xr.Dataset:
    lat = np.linspace(-40, -30, 8)
    lon = np.linspace(140, 150, 8)
    return xr.Dataset(
        {
            "GSLA": xr.DataArray(
                np.random.rand(8, 8),
                dims=["lat", "lon"],
                coords={"lat": lat, "lon": lon},
            )
        }
    )


# --- /{product}/{date}/{z}/{x}/{y}.png ---


def test_tile_unknown_product(client):
    response = client.get(
        "/api/v1/das/tiler/data_tiles/nonexistent/2024-01-01/1/0/0.png"
    )
    assert response.status_code == 404


def test_tile_bad_lod(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            return_value=_LOD_GRIDS,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            return_value=_make_ds(),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/99/0/0.png"
        )
    assert response.status_code == 404


def test_tile_out_of_bounds(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            return_value=_LOD_GRIDS,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            return_value=_make_ds(),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/1/5/5.png"
        )
    assert response.status_code == 404


def test_tile_missing_date(client):
    def _lod_grids_with_update(product):
        product.lod_grids.update(_LOD_GRIDS)
        return _LOD_GRIDS

    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            side_effect=_lod_grids_with_update,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            side_effect=FileNotFoundError("No data"),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/9999-01-01/1/0/0.png"
        )
    assert response.status_code == 404


def test_tile_ok(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            return_value=_LOD_GRIDS,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            return_value=_make_ds(),
        ),
        patch(
            "data_access_service.core.tiler_routes.data_tiles.render_tile",
            return_value=b"\x89PNG\r\n\x1a\n",
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/1/0/0.png"
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"


# --- /{product}/{date}/manifest.json ---


def test_manifest_unknown_product(client):
    response = client.get(
        "/api/v1/das/tiler/data_tiles/nonexistent/2024-01-01/manifest.json"
    )
    assert response.status_code == 404


def test_manifest_missing_date(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            return_value=_LOD_GRIDS,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            side_effect=FileNotFoundError("No data"),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/9999-01-01/manifest.json"
        )
    assert response.status_code == 404


def test_manifest_ok(client):
    payload = {
        "bounds": {"lonMin": 110.0, "lonMax": 160.0, "latMin": -50.0, "latMax": -10.0},
        "valueRange": [0.0, 1.0],
        "lods": {
            "1": {
                "grid": [2, 2],
                "chunkPx": [256, 256],
                "storedPx": [258, 258],
                "padding": 1,
            }
        },
    }
    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            return_value=_LOD_GRIDS,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            return_value=_make_ds(),
        ),
        patch(
            "data_access_service.core.tiler_routes.data_tiles.render_manifest",
            return_value=payload,
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/manifest.json"
        )
    assert response.status_code == 200
    assert response.json() == payload


def test_manifest_categorical_flag_fields_pass_through(client):
    # The response schema must surface flagValues/flagMeanings; otherwise Pydantic
    # silently drops them as unknown keys.
    payload = {
        "bounds": {"lonMin": 110.0, "lonMax": 160.0, "latMin": -50.0, "latMax": -10.0},
        "valueRange": [0.0, 4.0],
        "flagValues": [0, 1, 2, 3, 4],
        "flagMeanings": ["none", "moderate", "strong", "severe", "extreme"],
        "lods": {
            "1": {
                "grid": [2, 2],
                "chunkPx": [256, 256],
                "storedPx": [258, 258],
                "padding": 1,
            }
        },
    }
    with (
        patch(
            "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
            return_value=_LOD_GRIDS,
        ),
        patch(
            "data_access_service.core.tiler_routes.shared.load_slice",
            return_value=_make_ds(),
        ),
        patch(
            "data_access_service.core.tiler_routes.data_tiles.render_manifest",
            return_value=payload,
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/manifest.json"
        )
    assert response.status_code == 200
    body = response.json()
    assert body["flagValues"] == [0, 1, 2, 3, 4]
    assert body["flagMeanings"] == ["none", "moderate", "strong", "severe", "extreme"]


# --- /{product}/{date}/point ---


def test_point_unknown_product(client):
    response = client.get(
        "/api/v1/das/tiler/data_tiles/nonexistent/2024-01-01/point?lat=-35&lon=145"
    )
    assert response.status_code == 404


def test_point_missing_date(client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        side_effect=FileNotFoundError("No data"),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/9999-01-01/point?lat=-35&lon=145"
        )
    assert response.status_code == 404


def test_point_ok(client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/point?lat=-35&lon=145"
        )
    assert response.status_code == 200
    body = response.json()
    assert "lat" in body and "lon" in body and "variables" in body
    assert "GSLA" in body["variables"]


def test_point_out_of_bounds(client):
    # Fixture grid covers lat -40..-30, lon 140..150. A point well south of that
    # must 404 rather than silently snapping to the edge cell (method="nearest").
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/2024-01-01/point?lat=-55.46&lon=145"
        )
    assert response.status_code == 404


# --- /manifest (products availability) ---


def test_availability_ok(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=["2024-06-01", "2024-07-01"],
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
    assert response.status_code == 200
    body = response.json()
    assert body["products"] == {
        "product_a": {
            "available_dates": ["2024-06-01", "2024-07-01"],
            "full_date_range": {"start": "2024-06-01", "end": "2024-07-01"},
        }
    }
    assert "cache_version" in body


def test_availability_date_filters(client):
    all_dates = ["2024-01-01", "2024-06-01", "2024-09-01", "2024-12-01"]
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=all_dates,
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/manifest?from=2024-06-01&to=2024-09-01"
        )
    assert response.status_code == 200
    product = response.json()["products"]["product_a"]
    assert product["available_dates"] == ["2024-06-01", "2024-09-01"]
    # full_date_range spans the full dataset, not the from/to-filtered subset.
    assert product["full_date_range"] == {"start": "2024-01-01", "end": "2024-12-01"}


def test_availability_default_from_is_dataset_start(client):
    # With no `from`, dates are not clipped to a recent window — even old dates
    # (well before 3 months ago) are returned, starting from the dataset's earliest.
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=["2020-01-01"],
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
    assert response.status_code == 200
    product = response.json()["products"]["product_a"]
    assert product["available_dates"] == ["2020-01-01"]
    assert product["full_date_range"] == {"start": "2020-01-01", "end": "2020-01-01"}


def test_availability_no_dates_in_range(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=["2020-01-01"],
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest?to=2019-01-01")
    assert response.status_code == 200
    product = response.json()["products"]["product_a"]
    assert product["available_dates"] == []
    # No dates in range, but the product still has data, so full_date_range is populated.
    assert product["full_date_range"] == {"start": "2020-01-01", "end": "2020-01-01"}
