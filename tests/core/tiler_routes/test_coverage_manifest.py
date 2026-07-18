"""Route behaviour of /manifest?collection_id= (coverage discovery mode)."""

from unittest.mock import patch

_URL = "/api/v1/das/tiler/data_tiles/manifest"

_DOC = {
    "collectionId": "2ffccdad-1197-4e41-b412-a9033517cfb2",
    "cacheVersion": "cv2",
    "products": [
        {
            "id": "cov1",
            "times": [
                {"datetime": "2026-07-16T00:00:00+10:00", "dateKey": "2026-07-16"}
            ],
        }
    ],
}


def test_collection_discovery_ok(client):
    with patch(
        "data_access_service.core.tiler_routes.products.build_coverage_discovery",
        return_value=_DOC,
    ) as build:
        response = client.get(
            _URL, params={"collection_id": _DOC["collectionId"], "from": "2026-01-01"}
        )
    assert response.status_code == 200
    assert response.json() == _DOC
    build.assert_called_once_with(_DOC["collectionId"], "2026-01-01", None)
    assert "must-revalidate" in response.headers["cache-control"]
    assert response.headers["etag"]


def test_collection_discovery_etag_304_roundtrip(client):
    with patch(
        "data_access_service.core.tiler_routes.products.build_coverage_discovery",
        return_value=_DOC,
    ):
        first = client.get(_URL, params={"collection_id": _DOC["collectionId"]})
        again = client.get(
            _URL,
            params={"collection_id": _DOC["collectionId"]},
            headers={"If-None-Match": first.headers["etag"]},
        )
    assert again.status_code == 304
    assert again.content == b""


def test_unknown_collection_is_404(client):
    with patch(
        "data_access_service.core.tiler_routes.products.build_coverage_discovery",
        return_value=None,
    ):
        response = client.get(
            _URL, params={"collection_id": "00000000-0000-0000-0000-000000000000"}
        )
    assert response.status_code == 404


def test_manifest_without_collection_id_keeps_availability_shape(client):
    with patch(
        "data_access_service.core.tiler_routes.products.get_available_dates",
        return_value=["2026-07-16"],
    ):
        response = client.get(_URL)
    assert response.status_code == 200
    body = response.json()
    assert "products" in body and "cache_version" in body
    # availability mode: per-product availability dicts, not the discovery list
    for availability in body["products"].values():
        assert "available_dates" in availability
