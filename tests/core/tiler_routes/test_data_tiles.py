import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import CoastalFill, Product


def test_get_products_coastal_fill_null_when_absent(client, monkeypatch):
    monkeypatch.setitem(
        registry.PRODUCTS,
        "sparse",
        Product(
            id="sparse",
            source_path="s3://b/x.zarr",
            variable="GSLA",
            coastal_fill=CoastalFill(max_dist_px=4),
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
    assert by_id["sparse"]["coastal_fill"] == {"max_dist_px": 4}
    assert by_id["plain"]["coastal_fill"] is None


def test_get_products_reflects_effective_state(client, monkeypatch):
    """GET /products is built from live Product state, not the raw JSON —
    fields that products.json doesn't set explicitly still show their
    resolved value (e.g. ocean_masked's default-by-id rule)."""
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
    assert p["chunk_px"] == [240, 192]
    assert p["padding"] == 1
    # ocean_masked here comes from the Product's own default (False), since this
    # instance was constructed directly rather than via registry._from_dict —
    # the default-by-id rule lives in _from_dict, not on Product itself.
    assert p["ocean_masked"] is False


def test_list_products_metadata_uuid_null_when_absent(client, tmp_path, monkeypatch):
    cfg = tmp_path / "products.json"
    cfg.write_text(
        json.dumps(
            [
                {
                    "id": "linked",
                    "source_path": "s3://b/x.zarr",
                    "variable": "GSLA",
                    "metadata_uuid": "uuid-123",
                },
                {"id": "plain", "source_path": "s3://b/y.zarr", "variable": "V"},
            ]
        )
    )
    monkeypatch.setattr(registry, "_config_path", cfg)
    registry.load_products()

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


# --- /{product}/inspect ---


def _make_store_ds() -> xr.Dataset:
    """Full store dataset (with a time axis + chunk encoding) as returned by get_store."""
    lat = np.linspace(-40, -30, 4)
    lon = np.linspace(140, 150, 4)
    time = pd.to_datetime(["2024-01-01", "2024-01-02"])
    da = xr.DataArray(
        np.random.rand(2, 4, 4).astype("float32"),
        dims=["time", "lat", "lon"],
        coords={"time": time, "lat": lat, "lon": lon},
        attrs={
            "units": "m",
            "long_name": "Sea Level Anomaly",
            "_scale": np.float32(0.5),
        },
    )
    ds = xr.Dataset(
        {"GSLA": da}, attrs={"title": "IMOS SLA", "ranges": np.array([0.0, 1.0])}
    )
    ds["GSLA"].encoding["chunks"] = (1, 4, 4)
    return ds


def test_inspect_unknown_product(client):
    response = client.get("/api/v1/das/tiler/data_tiles/nonexistent/inspect")
    assert response.status_code == 404


def test_inspect_ok(client):
    with patch(
        "data_access_service.core.tiler_routes.products.get_store",
        return_value=_make_store_ds(),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/sea_level_anomaly/inspect")
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "sea_level_anomaly"
    assert body["dimensions"] == {"time": 2, "lat": 4, "lon": 4}

    gsla = body["variables"]["GSLA"]
    assert gsla["dimensions"] == ["time", "lat", "lon"]
    assert gsla["shape"] == [2, 4, 4]
    assert gsla["chunks"] == [1, 4, 4]
    assert gsla["dtype"] == "float32"
    assert gsla["units"] == "m"
    # numpy attrs must round-trip through JSON (np.float32 → float, np.ndarray → list).
    assert gsla["attributes"]["_scale"] == 0.5
    assert body["attributes"]["title"] == "IMOS SLA"
    assert body["attributes"]["ranges"] == [0.0, 1.0]


def test_inspect_missing_store(client):
    with patch(
        "data_access_service.core.tiler_routes.products.get_store",
        side_effect=FileNotFoundError("store gone"),
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/sea_level_anomaly/inspect")
    assert response.status_code == 404


def test_inspect_multi_variable(client):
    # ocean_current declares variables ["UCUR", "VCUR"]; inspect must report exactly
    # the product's declared variables.
    lat = np.linspace(-40, -30, 4)
    lon = np.linspace(140, 150, 4)
    coords = {"lat": lat, "lon": lon}
    ds = xr.Dataset(
        {
            "UCUR": xr.DataArray(
                np.random.rand(4, 4), dims=["lat", "lon"], coords=coords
            ),
            "VCUR": xr.DataArray(
                np.random.rand(4, 4), dims=["lat", "lon"], coords=coords
            ),
        }
    )
    with patch(
        "data_access_service.core.tiler_routes.products.get_store", return_value=ds
    ):
        response = client.get("/api/v1/das/tiler/data_tiles/ocean_current/inspect")
    assert response.status_code == 200
    assert set(response.json()["variables"]) == {"UCUR", "VCUR"}


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
    assert body["max_lods"] == 4


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
