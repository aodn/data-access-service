from dataclasses import replace

import pytest

from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import (
    DEFAULT_DATE,
    SubsetRequest,
)


def _valid_kwargs(**overrides):
    base = {
        "uuid": "test-uuid-123",
        "keys": ["key1"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "recipient": "test@example.com",
        "multi_polygon": None,
        "bboxes": [BoundingBox(min_lat=-10, max_lat=10, min_lon=-20, max_lon=20)],
    }
    base.update(overrides)
    return base


def test_valid_request_constructs():
    SubsetRequest(**_valid_kwargs())


def test_default_date_is_accepted_for_both_bounds():
    SubsetRequest(**_valid_kwargs(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE))


def test_one_default_one_concrete_is_accepted():
    SubsetRequest(**_valid_kwargs(start_date=DEFAULT_DATE))
    SubsetRequest(**_valid_kwargs(end_date=DEFAULT_DATE))


def test_empty_uuid_raises():
    with pytest.raises(ValueError, match="uuid"):
        SubsetRequest(**_valid_kwargs(uuid=""))


def test_empty_keys_raises():
    with pytest.raises(ValueError, match="keys"):
        SubsetRequest(**_valid_kwargs(keys=[]))


def test_recipient_without_at_sign_raises():
    with pytest.raises(ValueError, match="recipient"):
        SubsetRequest(**_valid_kwargs(recipient="not-an-email"))


def test_unknown_output_format_raises():
    with pytest.raises(ValueError, match="output_format"):
        SubsetRequest(**_valid_kwargs(output_format="parquet"))


def test_default_output_format_is_netcdf():
    req = SubsetRequest(**_valid_kwargs())
    assert req.output_format == "netcdf"


def test_unparseable_date_raises():
    with pytest.raises(ValueError, match="start_date"):
        SubsetRequest(**_valid_kwargs(start_date="not-a-date"))
    with pytest.raises(ValueError, match="end_date"):
        SubsetRequest(**_valid_kwargs(end_date="not-a-date"))


def test_end_before_start_raises():
    with pytest.raises(ValueError, match="on or before"):
        SubsetRequest(**_valid_kwargs(start_date="2024-12-31", end_date="2024-01-01"))


def test_replace_re_runs_validation():
    # subsetting.init relies on dataclasses.replace() to swap in resolved
    # default dates. That re-run of __post_init__ is what catches a bad
    # override mid-pipeline; this test guards against it being silently
    # dropped (e.g. if someone migrates to a non-validating wrapper).
    req = SubsetRequest(**_valid_kwargs())
    with pytest.raises(ValueError, match="recipient"):
        replace(req, recipient="not-an-email")
