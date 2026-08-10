"""Visual routes reject products DAS says cannot serve visual tiles.

Capability is explicit on the product now rather than inferred from variable
arity, so a registered *scalar* can legitimately be data-tile-only. That case
did not exist before: the arity narrowing accepts every scalar, so without this
check such a product would render a meaningless image instead of saying so.
"""

import pytest

from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS

NON_VISUAL_ID = "data_only"
VISUAL_ID = "sea_level_anomaly"
PAIR_ID = "ocean_current"

BASE = "/api/v1/das/tiler/visual_tiles"

# Every product-consuming endpoint under the visual-tiles router.
VISUAL_ROUTES = [
    pytest.param(f"{BASE}/{{pid}}/2024-01-01/5/0/0.png", id="tile"),
    pytest.param(f"{BASE}/{{pid}}/2024-01-01/bbox.png", id="bbox"),
    pytest.param(f"{BASE}/{{pid}}/2024-01-01/2024-01-02/animation.gif", id="animation"),
]


@pytest.fixture(autouse=True)
def non_visual_product(monkeypatch):
    """A scalar product that is registered but not renderable as a visual tile."""
    monkeypatch.setitem(
        PRODUCTS,
        NON_VISUAL_ID,
        Product(
            id=NON_VISUAL_ID,
            source_path="s3://test/sla.zarr",
            variable="WDIR",
            visual=False,
        ),
    )


@pytest.mark.parametrize("route", VISUAL_ROUTES)
def test_visual_route_rejects_a_non_visual_scalar(client, route):
    response = client.get(route.format(pid=NON_VISUAL_ID))

    assert response.status_code == 400
    assert "does not support visual tiles" in response.json()["detail"]


@pytest.mark.parametrize("route", VISUAL_ROUTES)
def test_rejection_happens_before_any_rendering(client, route, monkeypatch):

    def explode(*args, **kwargs):
        raise AssertionError("rendering must not be reached")

    monkeypatch.setattr(
        "data_access_service.core.tiler_routes.shared.load_slice", explode
    )

    assert client.get(route.format(pid=NON_VISUAL_ID)).status_code == 400


@pytest.mark.parametrize("route", VISUAL_ROUTES)
def test_unknown_product_is_still_a_404_not_a_400(client, route):
    assert client.get(route.format(pid="no_such_product")).status_code == 404


@pytest.mark.parametrize("route", VISUAL_ROUTES)
def test_vector_product_remains_rejected(client, route):
    assert client.get(route.format(pid=PAIR_ID)).status_code == 400


def test_point_endpoint_stays_open_to_data_capable_products(client, monkeypatch):
    """The raw point lookup is not a visual route: it reads values, it does not
    render, so a data-only product must still be able to answer it."""
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {
            "WDIR": xr.DataArray(
                np.zeros((2, 2)),
                dims=("lat", "lon"),
                coords={"lat": [-35.0, -34.0], "lon": [150.0, 151.0]},
            )
        }
    )
    monkeypatch.setattr(
        "data_access_service.core.tiler_routes.shared.load_slice",
        lambda *args, **kwargs: ds,
    )

    response = client.get(
        f"/api/v1/das/tiler/visual_tiles/{NON_VISUAL_ID}/2024-01-01/point"
        "?lat=-34.5&lon=150.5"
    )

    assert response.status_code == 200
    assert "WDIR" in response.json()["variables"]


def test_products_listing_exposes_the_capability(client):
    response = client.get("/api/v1/das/tiler/visual_tiles/products")

    assert response.status_code == 200
    by_id = {p["id"]: p for p in response.json()}
    assert by_id[NON_VISUAL_ID]["visual"] is False
    assert by_id[VISUAL_ID]["visual"] is True
    # metadata_uuid stays exposed for OGC collection filtering.
    assert "metadata_uuid" in by_id[NON_VISUAL_ID]


def test_visual_capable_scalar_is_not_rejected_by_the_new_check(client, monkeypatch):
    """Guard against the check being too eager: the default path must be
    unaffected, so failure here should come from rendering, never from 400."""

    def explode(*args, **kwargs):
        raise RuntimeError("reached rendering")

    monkeypatch.setattr(
        "data_access_service.core.tiler_routes.shared.load_slice", explode
    )

    with pytest.raises(RuntimeError, match="reached rendering"):
        client.get(f"{BASE}/{VISUAL_ID}/2024-01-01/5/0/0.png")
