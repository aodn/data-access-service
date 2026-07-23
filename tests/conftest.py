"""Shared test fixtures - auto-discovered by pytest."""

import pytest

from data_access_service.batch.subsetting.helpers.request_helper import (
    get_subset_request,
)

DEFAULT_MULTI_POLYGON = '{"type":"MultiPolygon","coordinates":[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}'
DEFAULT_CITATION = "Cite data as: Mazor, T., Watermeyer, K., Hobley, T., Grinter, V., Holden, R., MacDonald, K. and Ferns, L. (2023). Statewide Marine Habitat Map."


@pytest.fixture
def subset_request_factory():
    """Build a SubsetRequest via get_subset_request. Overrides use SubsetRequest field names."""

    def _factory(**overrides):
        defaults = {
            "uuid": "ffe8f19c-de4a-4362-89be-7605b2dd6b8c",
            "keys": ["radar_CoffsHarbour_wind_delayed_qc.zarr"],
            "start_date": "2000-01-01",
            "end_date": "2030-12-31",
            "recipient": "test@example.com",
            "multi_polygon": DEFAULT_MULTI_POLYGON,
            "collection_title": "Test Ocean Data Collection",
            "full_metadata_link": "https://metadata.imas.utas.edu.au/.../test",
            "suggested_citation": DEFAULT_CITATION,
            "output_format": "netcdf",
        }
        merged = {**defaults, **overrides}
        keys = merged.pop("keys")
        return get_subset_request({**merged, "key": ",".join(keys)})

    return _factory
