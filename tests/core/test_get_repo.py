"""Unit tests for the get_repo route dependency (product resolution + readiness).

get_repo only reads ``request.app.state.repositories`` and the repo's
``is_loaded()``, so a lightweight fake request/repo is enough — no FastAPI app,
DuckDB or S3 needed.
"""

from http import HTTPStatus
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from data_access_service.utils.routes_helper import get_repo


class _FakeRepo:
    def __init__(self, loaded: bool):
        self._loaded = loaded

    def is_loaded(self) -> bool:
        return self._loaded


def _request(repositories):
    # get_repo reaches request.app.state.repositories
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(repositories=repositories)))


def test_get_repo_returns_loaded_repository():
    repo = _FakeRepo(loaded=True)
    assert get_repo("mooring", _request({"mooring": repo})) is repo


def test_get_repo_unknown_product_404():
    with pytest.raises(HTTPException) as excinfo:
        get_repo("nope", _request({"mooring": _FakeRepo(True)}))
    assert excinfo.value.status_code == HTTPStatus.NOT_FOUND


def test_get_repo_not_loaded_503():
    with pytest.raises(HTTPException) as excinfo:
        get_repo("mooring", _request({"mooring": _FakeRepo(loaded=False)}))
    assert excinfo.value.status_code == HTTPStatus.SERVICE_UNAVAILABLE


def test_get_repo_missing_state_404():
    # No repositories registered at all (e.g. startup not finished).
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    with pytest.raises(HTTPException) as excinfo:
        get_repo("mooring", request)
    assert excinfo.value.status_code == HTTPStatus.NOT_FOUND
