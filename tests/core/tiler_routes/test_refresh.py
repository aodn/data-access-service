from unittest.mock import patch

from data_access_service.core.tiler_routes.startup import RefreshInProgressError
from data_access_service.tiler.services.product.product import Product

URL = "/api/v1/das/tiler/refresh"


def test_refresh_returns_summary(client):
    products = {"a:v": Product(id="a:v", store="a", variable="v")}
    outcomes = {"a": None, "b": RuntimeError("missing")}
    with patch(
        "data_access_service.core.tiler_routes.refresh.refresh_tiler",
        return_value=(products, outcomes),
    ) as refresh:
        resp = client.post(URL)

    refresh.assert_called_once()
    assert resp.status_code == 200
    assert resp.json() == {"products": 1, "stores": 2, "failed_stores": ["b"]}


def test_refresh_conflict_when_already_running(client):
    with patch(
        "data_access_service.core.tiler_routes.refresh.refresh_tiler",
        side_effect=RefreshInProgressError("busy"),
    ):
        resp = client.post(URL)

    assert resp.status_code == 409
