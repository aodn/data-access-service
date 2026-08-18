import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import (
    CoastalFill,
    DataTileConfig,
    Product,
)


def test_get_products_coastal_fill_null_when_absent(client, monkeypatch):
    monkeypatch.setitem(
        registry.PRODUCTS,
        "sparse",
        Product(
            id="sparse",
            source_path="s3://b/x.zarr",
            variable="GSLA",
            data_tile=DataTileConfig(coastal_fill=CoastalFill(max_dist_px=4)),
        ),
    )
    monkeypatch.setitem(
        registry.PRODUCTS,
        "plain",
        Product(id="plain", source_path="s3://b/y.zarr", variable="V"),
    )

    r = client.get("/api/v1/das/tiler/data_tiles/products")
    assert r.status_code == 200
    by_id = {p["id"]: p for p in r.json()}
    assert by_id["sparse"]["data_tile"]["coastal_fill"] == {"max_dist_px": 4}
    assert by_id["plain"]["data_tile"]["coastal_fill"] is None


def test_get_products_reflects_effective_state(client, monkeypatch):
    """GET /products is built from live Product state, so it shows each
    product's resolved configuration — including defaults nothing in config
    spelled out explicitly."""
    monkeypatch.setitem(
        registry.PRODUCTS,
        "currents",
        Product(
            id="model_sea_level_anomaly_gridded_realtime:ucur+vcur",
            source_path="s3://b/z.zarr",
            variable=["UCUR", "VCUR"],
        ),
    )

    r = client.get("/api/v1/das/tiler/data_tiles/products")
    assert r.status_code == 200
    by_id = {p["id"]: p for p in r.json()}
    p = by_id["model_sea_level_anomaly_gridded_realtime:ucur+vcur"]
    assert p["data_tile"]["chunk_px"] == [240, 192]
    assert p["data_tile"]["padding"] == 1
    # ocean_masked here is the Product's own default (False). In production the
    # real currents product gets True from the dataset override in
    # gridded_variables.json; this instance was constructed directly.
    assert p["ocean_masked"] is False


def test_list_products_metadata_uuid_null_when_absent(client, monkeypatch):
    """Derived products always carry the uuid they were discovered under, but
    the field stays optional on Product, so /products must still serialize the
    absent case as null rather than omitting it."""
    monkeypatch.setitem(
        registry.PRODUCTS,
        "linked",
        Product(
            id="linked",
            source_path="s3://b/x.zarr",
            variable="GSLA",
            metadata_uuid="uuid-123",
        ),
    )
    monkeypatch.setitem(
        registry.PRODUCTS,
        "plain",
        Product(id="plain", source_path="s3://b/y.zarr", variable="V"),
    )

    r = client.get("/api/v1/das/tiler/data_tiles/products")
    assert r.status_code == 200
    by_id = {p["id"]: p for p in r.json()}
    assert by_id["linked"]["metadata_uuid"] == "uuid-123"
    assert by_id["plain"]["metadata_uuid"] is None


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


# --- /{product}/{z}/{x}/{y}.png?date=... ---


def test_tile_unknown_product(client):
    response = client.get(
        "/api/v1/das/tiler/data_tiles/nonexistent/1/0/0.png?date=2024-01-01T00:00:00Z"
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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/99/0/0.png?date=2024-01-01T00:00:00Z"
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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/1/5/5.png?date=2024-01-01T00:00:00Z"
        )
    assert response.status_code == 404


def test_tile_missing_date(client):
    def _lod_grids_with_update(product):
        product.data_tile.lod_grids.update(_LOD_GRIDS)
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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/1/0/0.png?date=9999-01-01T00:00:00Z"
        )
    assert response.status_code == 404


def test_tile_missing_store(client):
    # get_lod_grids opens the store directly (get_store -> aodn_cloud_optimised) before
    # load_slice_or_404 ever runs, so a missing store must still surface as a
    # 404 via the app-level FileNotFoundError handler, not an unhandled 500.
    with patch(
        "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
        side_effect=FileNotFoundError(
            "No such file or directory: 's3://bucket/missing.zarr'"
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/1/0/0.png?date=2024-01-01T00:00:00Z"
        )
    assert response.status_code == 404
    assert "s3://bucket/missing.zarr" in response.json()["detail"]


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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/1/0/0.png?date=2024-01-01T00:00:00Z"
        )
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"


# --- /{product}/manifest.json?date=... ---


def test_manifest_unknown_product(client):
    response = client.get(
        "/api/v1/das/tiler/data_tiles/nonexistent/manifest.json?date=2024-01-01T00:00:00Z"
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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/manifest.json?date=9999-01-01T00:00:00Z"
        )
    assert response.status_code == 404


def test_manifest_missing_store(client):
    with patch(
        "data_access_service.core.tiler_routes.data_tiles.get_lod_grids",
        side_effect=FileNotFoundError(
            "No such file or directory: 's3://bucket/missing.zarr'"
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/manifest.json?date=2024-01-01T00:00:00Z"
        )
    assert response.status_code == 404
    assert "s3://bucket/missing.zarr" in response.json()["detail"]


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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/manifest.json?date=2024-01-01T00:00:00Z"
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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/manifest.json?date=2024-01-01T00:00:00Z"
        )
    assert response.status_code == 200
    body = response.json()
    assert body["flagValues"] == [0, 1, 2, 3, 4]
    assert body["flagMeanings"] == ["none", "moderate", "strong", "severe", "extreme"]


# --- /{product}/point?date=... ---


def test_point_unknown_product(client):
    response = client.get(
        "/api/v1/das/tiler/data_tiles/nonexistent/point?date=2024-01-01T00:00:00Z&lat=-35&lon=145"
    )
    assert response.status_code == 404


def test_point_missing_date(client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        side_effect=FileNotFoundError("No data"),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/point?date=9999-01-01T00:00:00Z&lat=-35&lon=145"
        )
    assert response.status_code == 404


def test_point_ok(client):
    with patch(
        "data_access_service.core.tiler_routes.shared.load_slice",
        return_value=_make_ds(),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/point?date=2024-01-01T00:00:00Z&lat=-35&lon=145"
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
            "/api/v1/das/tiler/data_tiles/sea_level_anomaly/point?date=2024-01-01T00:00:00Z&lat=-55.46&lon=145"
        )
    assert response.status_code == 404


# --- /manifest (products availability) ---


def _avail(dates: list[str]) -> list[tuple[str, pd.Timestamp]]:
    """Build a get_available_dates-shaped [(iso_string, timestamp), ...] fixture."""
    return [(d, pd.Timestamp(d)) for d in dates]


def test_availability_ok(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=_avail(["2024-06-01", "2024-07-01"]),
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
    assert "max_lods" not in body
    assert "data_tile" not in body


def test_availability_date_filters(client):
    all_dates = ["2024-01-01", "2024-06-01", "2024-09-01", "2024-12-01"]
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=_avail(all_dates),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/manifest?from=2024-06-01T00:00:00Z&to=2024-09-01T00:00:00Z"
        )
    assert response.status_code == 200
    product = response.json()["products"]["product_a"]
    assert product["available_dates"] == ["2024-06-01", "2024-09-01"]
    # full_date_range spans the full dataset, not the from/to-filtered subset.
    assert product["full_date_range"] == {"start": "2024-01-01", "end": "2024-12-01"}


def test_availability_no_from_is_unbounded(client):
    # With no `from`, nothing is excluded on the lower end — from defaults to
    # no lower bound, not a fixed window.
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=_avail(["2020-01-01"]),
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
    assert response.status_code == 200
    product = response.json()["products"]["product_a"]
    assert product["available_dates"] == ["2020-01-01"]
    assert product["full_date_range"] == {"start": "2020-01-01", "end": "2020-01-01"}


def test_availability_metadata_uuid_filters_to_matching_products(client):
    products = {
        "product_a": Product(
            id="product_a",
            source_path="s3://bucket/a.zarr",
            variable="VAR",
            metadata_uuid="uuid-1",
        ),
        "product_b": Product(
            id="product_b",
            source_path="s3://bucket/b.zarr",
            variable="VAR",
            metadata_uuid="uuid-2",
        ),
    }
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(products.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=_avail(["2024-01-01"]),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/manifest?metadata_uuid=uuid-1"
        )
    assert response.status_code == 200
    products_out = response.json()["products"]
    assert "product_a" in products_out
    assert "product_b" not in products_out


def test_availability_metadata_uuid_no_match_is_404(client):
    with patch(
        "data_access_service.core.tiler_routes.products.iter_product_items",
        return_value=list(_FAKE_PRODUCTS.items()),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/manifest?metadata_uuid=does-not-exist"
        )
    assert response.status_code == 404
    assert "does-not-exist" in response.json()["detail"]


def test_availability_no_metadata_uuid_returns_every_product(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=_avail(["2024-01-01"]),
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
    assert response.status_code == 200
    assert "product_a" in response.json()["products"]


def test_availability_missing_store(client):
    # get_available_dates opens the store directly and is never wrapped by
    # load_slice_or_404, so a missing store must still 404, not 500.
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            side_effect=FileNotFoundError(
                "No such file or directory: 's3://bucket/missing.zarr'"
            ),
        ),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/manifest")
    assert response.status_code == 404
    assert "s3://bucket/missing.zarr" in response.json()["detail"]


def test_availability_no_dates_in_range(client):
    with (
        patch(
            "data_access_service.core.tiler_routes.products.iter_product_items",
            return_value=list(_FAKE_PRODUCTS.items()),
        ),
        patch(
            "data_access_service.core.tiler_routes.products.get_available_dates",
            return_value=_avail(["2020-01-01"]),
        ),
    ):
        response = client.get(
            "/api/v1/das/tiler/data_tiles/manifest?to=2019-01-01T00:00:00Z"
        )
    assert response.status_code == 200
    product = response.json()["products"]["product_a"]
    assert product["available_dates"] == []
    # No dates in range, but the product still has data, so full_date_range is populated.
    assert product["full_date_range"] == {"start": "2020-01-01", "end": "2020-01-01"}
