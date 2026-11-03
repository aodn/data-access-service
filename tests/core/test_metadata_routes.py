"""GET /metadata/{uuid}: the keys of a collection, cloud-optimised or not."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from data_access_service.config.config import Config
from data_access_service.core.descriptor import Descriptor
from data_access_service.core.routes import metadata as metadata_routes
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.server import app

URL = f"{Config.BASE_URL}/metadata/uuid-1"
CO_ONLY = {"include_data_index": "false"}
NOT_EXIST = {"not_exist": Descriptor(uuid="uuid-1")}


@pytest.fixture
def api():
    """A ready API instance, with auth bypassed."""
    mock = MagicMock()
    mock.get_api_status.return_value = True
    app.dependency_overrides[api_key_auth] = lambda: "test-key"
    with patch("data_access_service.server.API", return_value=mock):
        yield mock
    app.dependency_overrides.clear()


@pytest.fixture
def client(api):
    with TestClient(app) as c:
        yield c


def _in_index(monkeypatch, found=True):
    """Whether the data index has NetCDF files for the uuid."""
    lookup = MagicMock(return_value=found)
    monkeypatch.setattr(metadata_routes, "is_data_index_uuid", lookup)
    return lookup


def test_cloud_optimised_keys_have_their_type(monkeypatch, api, client):
    api.get_mapped_meta_data.return_value = {
        "a.parquet": Descriptor(uuid="uuid-1", dname="a.parquet")
    }
    lookup = _in_index(monkeypatch)

    response = client.get(URL)

    assert response.status_code == 200
    # Same shape as before, plus the new type.
    assert response.json() == {
        "a.parquet": {
            "uuid": "uuid-1",
            "dname": "a.parquet",
            "lat": None,
            "lng": None,
            "depth": None,
            "type": "cloud_optimised",
        }
    }
    # A collection with cloud-optimised data uses those keys only.
    lookup.assert_not_called()


def test_no_cloud_optimised_data_is_not_found_when_turned_off(monkeypatch, api, client):
    # A caller that wants cloud-optimised keys only gets the 404 as before.
    api.get_mapped_meta_data.return_value = NOT_EXIST
    lookup = _in_index(monkeypatch)

    assert client.get(URL, params=CO_ONLY).status_code == 404
    lookup.assert_not_called()


def test_no_cloud_optimised_data_returns_the_original_netcdfs_key(
    monkeypatch, api, client
):
    api.get_mapped_meta_data.return_value = NOT_EXIST
    _in_index(monkeypatch)

    response = client.get(URL)

    assert response.status_code == 200
    assert response.json() == {
        "original_netcdfs": {
            "uuid": "uuid-1",
            "dname": "original_netcdfs",
            "lat": None,
            "lng": None,
            "depth": None,
            "type": "data_index",
        }
    }


def test_uuid_not_in_the_index_is_not_found(monkeypatch, api, client):
    api.get_mapped_meta_data.return_value = NOT_EXIST
    _in_index(monkeypatch, found=False)

    assert client.get(URL).status_code == 404
