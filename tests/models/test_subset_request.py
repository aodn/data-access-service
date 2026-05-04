from dataclasses import replace

import pytest

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import (
    NON_SPECIFIED_DATE,
    SubsetRequest,
)


def _valid_kwargs(**overrides):
    base = {
        "uuid": "test-uuid-123",
        "keys": ["key1"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "recipient": "test@example.com",
        "output_format": "netcdf",
        "multi_polygon": None,
        "bboxes": [BoundingBox(min_lat=-10, max_lat=10, min_lon=-20, max_lon=20)],
    }
    base.update(overrides)
    return base


class TestSubsetRequestValidation:
    """Output validation: rules SubsetRequest enforces on its own fields."""

    def test_valid_request_constructs(self):
        SubsetRequest(**_valid_kwargs())

    def test_default_date_is_accepted_for_both_bounds(self):
        SubsetRequest(
            **_valid_kwargs(start_date=NON_SPECIFIED_DATE, end_date=NON_SPECIFIED_DATE)
        )

    def test_mixed_default_marker_and_concrete_date_is_accepted(self):
        SubsetRequest(**_valid_kwargs(start_date=NON_SPECIFIED_DATE))
        SubsetRequest(**_valid_kwargs(end_date=NON_SPECIFIED_DATE))

    def test_empty_uuid_raises(self):
        with pytest.raises(ValueError, match="uuid"):
            SubsetRequest(**_valid_kwargs(uuid=""))

    def test_empty_keys_raises(self):
        with pytest.raises(ValueError, match="keys"):
            SubsetRequest(**_valid_kwargs(keys=[]))

    def test_recipient_without_at_sign_raises(self):
        with pytest.raises(ValueError, match="recipient"):
            SubsetRequest(**_valid_kwargs(recipient="not-an-email"))

    def test_unknown_output_format_raises(self):
        with pytest.raises(ValueError, match="output_format"):
            SubsetRequest(**_valid_kwargs(output_format="parquet"))

    def test_csv_output_format_is_supported(self):
        req = SubsetRequest(**_valid_kwargs(output_format="csv"))
        assert req.output_format == "csv"

    def test_geotiff_output_format_is_supported(self):
        req = SubsetRequest(**_valid_kwargs(output_format="geotiff"))
        assert req.output_format == "geotiff"

    def test_unparseable_date_raises(self):
        with pytest.raises(ValueError, match="start_date"):
            SubsetRequest(**_valid_kwargs(start_date="not-a-date"))
        with pytest.raises(ValueError, match="end_date"):
            SubsetRequest(**_valid_kwargs(end_date="not-a-date"))

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError, match="on or before"):
            SubsetRequest(
                **_valid_kwargs(start_date="2024-12-31", end_date="2024-01-01")
            )

    def test_dataclasses_replace_re_runs_validation(self):
        # subsetting.init relies on dataclasses.replace() to swap in resolved
        # default dates. That re-run of __post_init__ is what catches a bad
        # override mid-pipeline; this test guards against it being silently
        # dropped (e.g. if someone migrates to a non-validating wrapper).
        req = SubsetRequest(**_valid_kwargs())
        with pytest.raises(ValueError, match="recipient"):
            replace(req, recipient="not-an-email")
