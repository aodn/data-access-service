"""Unit tests for the Step 5/6 routing in subsetting_main.init.

The requested format decides the workflow, because it implies the storage type:
csv is produced from parquet keys (legacy workflow), netcdf/geotiff from zarr
keys (ZarrProcessor). A key that cannot produce the requested format is
rejected before any child job is submitted, the same pairing the size estimate
rejects - the legacy workflow's zarr-writing code that used to absorb the
mismatch has been removed.
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock

import data_access_service.batch.subsetting.subsetting_main as subsetting_main
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.subset_request_resolver import ResolvedSubsetRequest

UUID = "test-uuid"


def _subset_request(keys: list[str], output_format: str) -> SubsetRequest:
    return SubsetRequest(
        uuid=UUID,
        keys=keys,
        start_date="non-specified",
        end_date="non-specified",
        recipient="user@example.com",
        output_format=output_format,
    )


def _resolved(keys: list[str]) -> ResolvedSubsetRequest:
    return ResolvedSubsetRequest(
        uuid=UUID,
        keys=keys,
        start_date=pd.Timestamp("2020-01-01", tz="UTC"),
        end_date=pd.Timestamp("2020-01-05", tz="UTC"),
        bboxes=[],
        columns=None,
        geometry=None,
    )


def _run_init(monkeypatch, keys: list[str], output_format: str) -> MagicMock:
    """Drive init() to the Step 5/6 routing with the request parsing stubbed
    out, and return the mocked run_zarr_subset so the caller can assert which
    workflow ran."""
    request = _subset_request(keys, output_format)
    monkeypatch.setattr(
        subsetting_main, "get_subset_request", MagicMock(return_value=request)
    )
    monkeypatch.setattr(
        subsetting_main, "normalize_request", MagicMock(return_value=request)
    )
    monkeypatch.setattr(
        subsetting_main,
        "resolve_subset_request",
        MagicMock(return_value=_resolved(keys)),
    )
    mock_run_zarr_subset = MagicMock()
    monkeypatch.setattr(subsetting_main, "run_zarr_subset", mock_run_zarr_subset)
    # The legacy path would otherwise construct real boto3 clients.
    monkeypatch.setattr(subsetting_main, "AWSHelper", MagicMock())

    subsetting_main.init(api=MagicMock(), job_id_of_init="job-1", parameters={})
    return mock_run_zarr_subset


@pytest.mark.parametrize("output_format", ["netcdf", "geotiff"])
def test_zarr_key_uses_zarr_workflow(monkeypatch, output_format):
    run_zarr_subset = _run_init(monkeypatch, ["a.zarr"], output_format)

    run_zarr_subset.assert_called_once()


def test_parquet_key_with_csv_uses_legacy_workflow(monkeypatch):
    run_zarr_subset = _run_init(monkeypatch, ["a.parquet"], "csv")

    run_zarr_subset.assert_not_called()


@pytest.mark.parametrize("output_format", ["netcdf", "geotiff"])
def test_parquet_key_with_zarr_only_format_raises(monkeypatch, output_format):
    # The legacy workflow would happily write a CSV zip here, ignoring the
    # requested format - so the user would get a file they did not ask for.
    # The size estimate rejects this pairing, so the download must too.
    with pytest.raises(ValueError, match="not possible"):
        _run_init(monkeypatch, ["a.parquet"], output_format)


def test_zarr_key_with_csv_raises(monkeypatch):
    # ZarrProcessor has no csv handler; without this check it would fail deep
    # inside the job instead of before any work is submitted.
    with pytest.raises(ValueError, match="not possible"):
        _run_init(monkeypatch, ["a.zarr"], "csv")


def test_mixed_zarr_and_parquet_keys_raise(monkeypatch):
    # A uuid can hold both storage types, so a request spanning both is
    # possible in principle; whichever format is asked for, one side of the mix
    # cannot produce it.
    with pytest.raises(ValueError, match="not possible"):
        _run_init(monkeypatch, ["a.zarr", "b.parquet"], "netcdf")


def test_key_of_unknown_storage_type_is_not_rejected(monkeypatch):
    # API.get_mapped_meta_data answers an unknown uuid with a "not_exist"
    # sentinel key, which resolve_keys expands "*" into. It carries no suffix,
    # so nothing can be concluded about its storage type - it must fall through
    # to the legacy workflow to report, not be rejected as a format mismatch.
    run_zarr_subset = _run_init(monkeypatch, ["not_exist"], "netcdf")

    run_zarr_subset.assert_not_called()
