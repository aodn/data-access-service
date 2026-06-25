"""Unit tests for SiteFeatureService.

The service only orchestrates: parse the ISO date params, call the repository,
shape the rows via the geojson helpers. So a lightweight fake repository that
records the args it receives and returns canned DataFrames is enough — no
FastAPI, DuckDB or S3 needed. This is the testability win of pulling the logic
out of the route handlers.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi import HTTPException

from data_access_service.sites.site_feature_service import SiteFeatureService


class _FakeRepo:
    """Records the args each read receives and returns whatever it's told to."""

    site_column = "site_code"
    time_column = "TIME"
    latitude_column = "LATITUDE"
    longitude_column = "LONGITUDE"
    value_columns = ("TEMP", "PSAL")
    group_column = None

    def __init__(self, *, sites=None, latest=None, details=None):
        self._sites = sites
        self._latest = latest
        self._details = details
        self.calls = {}

    def sites_in_date_range(self, start, end):
        self.calls["sites_in_date_range"] = (start, end)
        return self._sites

    def latest_time(self):
        self.calls["latest_time"] = ()
        return self._latest

    def site_details(self, site, start, end):
        self.calls["site_details"] = (site, start, end)
        return self._details


def test_sites_with_data_between_normalizes_dates_and_shapes_rows():
    rows = pd.DataFrame(
        {
            "site_code": ["A"],
            "latitude": [-42.0],
            "longitude": [147.0],
            "time": [pd.Timestamp("2024-01-02T03:00:00")],
        }
    )
    repo = _FakeRepo(sites=rows)
    service = SiteFeatureService(repo)

    result = service.sites_with_data_between("2024-01-01T00:00:00Z", None)

    # end stays None ("no bound"); start is normalized to naive UTC.
    assert repo.calls["sites_in_date_range"] == ("2024-01-01T00:00:00", None)
    assert result["type"] == "FeatureCollection"
    assert result["features"][0]["properties"]["site"] == "A"


def test_latest_time_wraps_value():
    repo = _FakeRepo(latest=pd.Timestamp("2024-05-06T07:08:09"))
    assert SiteFeatureService(repo).latest_time() == {"time": "2024-05-06T07:08:09"}


def test_latest_time_none_passthrough():
    assert SiteFeatureService(_FakeRepo(latest=None)).latest_time() == {"time": None}


def test_site_details_passes_site_and_dates_and_plumbs_columns():
    rows = pd.DataFrame(
        {
            "TIME": [pd.Timestamp("2024-01-01T00:00:00")],
            "LATITUDE": [-42.0],
            "LONGITUDE": [147.0],
            "TEMP": [12.5],
            "PSAL": [35.0],
        }
    )
    repo = _FakeRepo(details=rows)
    service = SiteFeatureService(repo)

    result = service.site_details("A", None, "2024-12-31T23:59:59Z")

    assert repo.calls["site_details"] == ("A", None, "2024-12-31T23:59:59")
    # ungrouped: the value-column series sit directly in properties.
    assert "TEMP" in result["properties"]
    assert "PSAL" in result["properties"]


def test_invalid_date_raises_http_400():
    service = SiteFeatureService(_FakeRepo())
    with pytest.raises(HTTPException) as excinfo:
        service.sites_with_data_between("not-a-date", None)
    assert excinfo.value.status_code == 400
