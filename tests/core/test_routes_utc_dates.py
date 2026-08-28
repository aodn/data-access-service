"""Route-level date handling for issue 9061.

Two things the pinned process timezone cannot fix: a default worked out once at
import time, and an output format that does not say "UTC" in a way JS parses by
spec.
"""

import inspect
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from data_access_service.config.config import Config
from data_access_service.core.routes import data as data_routes
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.routes.helpers import (
    require_api_ready,
    resolve_end_date_param,
)
from data_access_service.server import app

BASE = Config.BASE_URL


@pytest.fixture
def api():
    """A ready API instance, with auth and readiness checks bypassed."""
    mock = MagicMock()
    app.dependency_overrides[require_api_ready] = lambda: mock
    app.dependency_overrides[api_key_auth] = lambda: "test-key"
    yield mock
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    with patch("data_access_service.server.API"):
        with TestClient(app) as c:
            yield c


class TestEndDateDefaultIsPerRequest:
    """The default used to be datetime.now() in the signature, so it was frozen
    at import: a server up for a week answered with the date it started on. It
    also had no fractional seconds, so verify_datatime_param rejected it and
    omitting end_date returned 400."""

    def test_omitting_end_date_is_accepted(self, api, client):
        api.has_data.return_value = True
        response = client.get(f"{BASE}/data/some-uuid/some-key/has_data")
        assert response.status_code == 200

    def test_omitted_end_date_resolves_to_now(self, api, client):
        api.has_data.return_value = True
        before = pd.Timestamp.now(tz="UTC")
        client.get(f"{BASE}/data/some-uuid/some-key/has_data")
        after = pd.Timestamp.now(tz="UTC")

        end_date = api.has_data.call_args.args[3]
        assert end_date.tz is not None
        assert before <= end_date <= after

    def test_no_end_date_default_is_computed_at_import(self):
        """Guards the regression: anything evaluated in the signature is
        evaluated once, when the module is imported."""
        for handler in (data_routes.has_data, data_routes.get_data):
            default = inspect.signature(handler).parameters["end_date"].default
            # Query(default=...) wraps the value, a bare default does not
            assert getattr(default, "default", default) is None

    def test_explicit_end_date_is_still_validated(self, api, client):
        # nanosecond precision is required for end_date
        response = client.get(
            f"{BASE}/data/some-uuid/some-key/has_data",
            params={"end_date": "2024-01-15"},
        )
        assert response.status_code == 400

    def test_resolve_end_date_param_follows_the_clock(self):
        first = resolve_end_date_param(None)
        second = resolve_end_date_param(None)
        assert first.tz is not None
        assert second >= first

    def test_resolve_end_date_param_accepts_an_offset(self):
        result = resolve_end_date_param("2024-01-15T10:00:00.123456789+10:00")
        assert result == pd.Timestamp("2024-01-15T00:00:00.123456789Z")


class TestTemporalExtentSendsZ:
    """strftime("%z") wrote "+0000" - no colon, so not one of the two forms the
    ECMAScript date-time format defines."""

    def test_renders_z(self, api, client):
        api.get_temporal_extent.return_value = (
            pd.Timestamp("2024-01-15T00:00:00"),
            pd.Timestamp("2024-06-30T23:59:59"),
        )
        response = client.get(f"{BASE}/data/some-uuid/some-key/temporal_extent")

        assert response.status_code == 200
        assert json.loads(response.content) == [
            {"start_date": "2024-01-15T00:00:00Z", "end_date": "2024-06-30T23:59:59Z"}
        ]

    def test_a_non_utc_extent_is_converted(self, api, client):
        api.get_temporal_extent.return_value = (
            pd.Timestamp("2024-01-15T10:00:00+10:00"),
            pd.Timestamp("2024-06-30T10:00:00+10:00"),
        )
        response = client.get(f"{BASE}/data/some-uuid/some-key/temporal_extent")

        assert json.loads(response.content) == [
            {"start_date": "2024-01-15T00:00:00Z", "end_date": "2024-06-30T00:00:00Z"}
        ]
