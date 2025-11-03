"""Shared test fixtures - auto-discovered by pytest."""

import pytest
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import SubsetRequest


@pytest.fixture
def subset_request_factory():
    """Factory to create SubsetRequest instances with custom or default values."""
    def _factory(**kwargs):
        defaults = {
            'uuid': 'test-uuid-123',
            'keys': ['key1', 'key2'],
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'bboxes': [BoundingBox(min_lat=-10, max_lat=10, min_lon=-20, max_lon=20)],
            'recipient': 'test@example.com',
            'collection_title': 'Test Collection',
            'full_metadata_link': 'https://example.com/metadata',
            'suggested_citation': 'Test Citation (2024)'
        }
        return SubsetRequest(**{**defaults, **kwargs})
    return _factory