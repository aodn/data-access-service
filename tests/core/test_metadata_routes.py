"""GET /metadata/{uuid}: the keys of a collection, cloud-optimised and data-index."""

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

CO_KEY_JSON = {
    "uuid": "uuid-1",
    "dname": "a.parquet",
    "lat": None,
    "lng": None,
    "depth": None,
    "type": "cloud_optimised",
}
DATA_INDEX_KEY_JSON = {
    "uuid": "uuid-1",
    "dname": "original_netcdfs",
    "lat": None,
    "lng": None,
    "depth": None,
    "type": "data_index",
}


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


def _co_keys():
    """The cloud-optimised keys of uuid-1, as the API caches them."""
    return {"a.parquet": Descriptor(uuid="uuid-1", dname="a.parquet")}


def _in_index(monkeypatch, found=True):
    """Whether the data index has NetCDF files for the uuid."""
    lookup = MagicMock(return_value=found)
    monkeypatch.setattr(metadata_routes, "is_data_index_uuid", lookup)
    return lookup


# The four cases of a collection: CO data and/or data-index files.


def test_co_data_and_data_index_files_return_both_kinds_of_key(
    monkeypatch, api, client
):
    cached = _co_keys()
    api.get_mapped_meta_data.return_value = cached
    _in_index(monkeypatch)

    response = client.get(URL)

    assert response.status_code == 200
    assert response.json() == {
        "a.parquet": CO_KEY_JSON,
        "original_netcdfs": DATA_INDEX_KEY_JSON,
    }
    # The cached keys of the API are not changed.
    assert list(cached) == ["a.parquet"]


def test_co_data_only_returns_the_co_keys(monkeypatch, api, client):
    api.get_mapped_meta_data.return_value = _co_keys()
    _in_index(monkeypatch, found=False)

    response = client.get(URL)

    assert response.status_code == 200
    assert response.json() == {"a.parquet": CO_KEY_JSON}


def test_data_index_files_only_return_the_original_netcdfs_key(
    monkeypatch, api, client
):
    api.get_mapped_meta_data.return_value = NOT_EXIST
    _in_index(monkeypatch)

    response = client.get(URL)

    assert response.status_code == 200
    assert response.json() == {"original_netcdfs": DATA_INDEX_KEY_JSON}


def test_no_co_data_and_no_data_index_files_is_not_found(monkeypatch, api, client):
    api.get_mapped_meta_data.return_value = NOT_EXIST
    _in_index(monkeypatch, found=False)

    assert client.get(URL).status_code == 404


# include_data_index=false: cloud-optimised keys only.


def test_co_keys_only_when_data_index_is_turned_off(monkeypatch, api, client):
    api.get_mapped_meta_data.return_value = _co_keys()
    lookup = _in_index(monkeypatch)

    response = client.get(URL, params=CO_ONLY)

    assert response.status_code == 200
    assert response.json() == {"a.parquet": CO_KEY_JSON}
    lookup.assert_not_called()


def test_no_co_data_is_not_found_when_data_index_is_turned_off(
    monkeypatch, api, client
):
    api.get_mapped_meta_data.return_value = NOT_EXIST
    lookup = _in_index(monkeypatch)

    assert client.get(URL, params=CO_ONLY).status_code == 404
    lookup.assert_not_called()


def test_list_of_all_collections_has_co_keys_only(monkeypatch, api, client):
    # GET /metadata (no uuid) is not changed: no data-index key is added.
    api.get_mapped_meta_data.return_value = {"uuid-1": _co_keys()}
    lookup = _in_index(monkeypatch)

    response = client.get(f"{Config.BASE_URL}/metadata")

    assert response.status_code == 200
    assert response.json() == {"uuid-1": {"a.parquet": CO_KEY_JSON}}
    lookup.assert_not_called()
