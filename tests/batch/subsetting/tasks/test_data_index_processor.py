"""DataIndexProcessor: query result -> zipped CSV file list + email."""

import csv
import io
import zipfile
from dataclasses import replace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_access_service.batch.subsetting.tasks import data_index_processor
from data_access_service.batch.subsetting.tasks.data_index_processor import (
    FILE_LIST_COLUMNS,
    DataIndexProcessor,
)
from data_access_service.core.data_index_files import ORIGINAL_NETCDF_KEY
from data_access_service.models.data_index_types import DataIndexFile
from data_access_service.models.subset_request import NON_SPECIFIED, SubsetRequest
from data_access_service.utils.email_templates.no_data_email import (
    NO_DATA_EMAIL_SUBJECT,
)

UUID = "uuid-1"
LIST_URL = "https://bucket.s3.ap-southeast-2.amazonaws.com/job-1/list.zip"

FILE = DataIndexFile(
    bucket="imos-data",
    key="IMOS/SRS/a file.nc",
    time_coverage_start=pd.Timestamp("2020-01-01", tz="UTC"),
    time_coverage_end=pd.Timestamp("2020-01-02", tz="UTC"),
    lat_min=-40.0,
    lat_max=-30.0,
    lon_min=140.0,
    lon_max=150.0,
)


def _request(keys=("*",)):
    return SubsetRequest(
        uuid=UUID,
        keys=list(keys),
        start_date="2020-01-01",
        end_date="2020-01-31",
        recipient="user@example.com",
        output_format="netcdf",
        collection_title="SRS Altimetry",
    )


@pytest.fixture
def aws(monkeypatch):
    aws = MagicMock()
    monkeypatch.setattr(data_index_processor, "AWSHelper", lambda: aws)
    # No S3 Table in tests.
    monkeypatch.setattr(data_index_processor, "DataIndexDuckDBClient", MagicMock())
    return aws


def _found(monkeypatch, files):
    """The index finds ``files`` for the uuid."""
    find = MagicMock(return_value=files)
    monkeypatch.setattr(data_index_processor, "find_data_index_files", find)
    return find


def _uploaded_keys(aws):
    return [c.args[2] for c in aws.upload_file_to_s3.call_args_list]


def test_no_files_sends_no_data_email(monkeypatch, aws):
    _found(monkeypatch, [])

    DataIndexProcessor(job_id="job-1", subset_request=_request()).process()

    aws.upload_file_to_s3.assert_not_called()
    aws.send_email.assert_called_once()
    assert aws.send_email.call_args.kwargs["subject"] == NO_DATA_EMAIL_SUBJECT


def test_files_are_uploaded_as_zipped_csv_and_emailed(monkeypatch, aws):
    find = _found(monkeypatch, [FILE])
    uploaded = {}

    def upload(path, bucket, key):
        # Read the zip now: it is in a temp dir that is gone after the call.
        with zipfile.ZipFile(path) as zf:
            uploaded["names"] = zf.namelist()
            uploaded["rows"] = list(
                csv.reader(io.TextIOWrapper(zf.open(zf.namelist()[0]), "utf-8"))
            )
        uploaded["key"] = key
        return LIST_URL

    aws.upload_file_to_s3.side_effect = upload

    DataIndexProcessor(job_id="job-1", subset_request=_request()).process()

    # Dates of the request, as UTC bounds; the end covers its whole day.
    kwargs = find.call_args.kwargs
    assert kwargs["uuid"] == UUID
    assert kwargs["start"] == pd.Timestamp("2020-01-01", tz="UTC")
    assert kwargs["end"] == pd.Timestamp("2020-01-31 23:59:59.999999999", tz="UTC")

    assert uploaded["key"] == "job-1/SRS_Altimetry-file-list.zip"
    assert uploaded["names"] == ["SRS_Altimetry-file-list.csv"]
    header, row = uploaded["rows"]
    assert header == FILE_LIST_COLUMNS
    assert row[0] == (
        "https://imos-data.s3.ap-southeast-2.amazonaws.com/IMOS/SRS/a%20file.nc"
    )
    assert row[1] == "IMOS/SRS/a file.nc"

    aws.send_email.assert_called_once()
    assert LIST_URL in aws.send_email.call_args.kwargs["html_body"]
    # The email shows the actual key, not "*".
    assert ORIGINAL_NETCDF_KEY in aws.send_email.call_args.kwargs["html_body"]


def test_no_time_set_means_no_time_filter(monkeypatch, aws):
    find = _found(monkeypatch, [FILE])
    aws.upload_file_to_s3.return_value = LIST_URL
    request = replace(_request(), start_date=NON_SPECIFIED, end_date=NON_SPECIFIED)

    DataIndexProcessor(job_id="job-1", subset_request=request).process()

    # Not 1970..today, which would drop the files with no time in the index.
    assert find.call_args.kwargs["start"] is None
    assert find.call_args.kwargs["end"] is None


def test_file_with_no_time_or_bbox_has_empty_cells(monkeypatch, aws):
    no_time = replace(
        FILE,
        time_coverage_start=None,
        time_coverage_end=None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
    )
    _found(monkeypatch, [no_time])
    rows = {}

    def upload(path, bucket, key):
        with zipfile.ZipFile(path) as zf:
            rows["all"] = list(
                csv.reader(io.TextIOWrapper(zf.open(zf.namelist()[0]), "utf-8"))
            )
        return LIST_URL

    aws.upload_file_to_s3.side_effect = upload

    DataIndexProcessor(job_id="job-1", subset_request=_request()).process()

    _, row = rows["all"]
    assert row[2:] == ["", "", "", "", "", ""]


def test_the_original_netcdfs_key_is_listed(monkeypatch, aws):
    find = _found(monkeypatch, [FILE])
    aws.upload_file_to_s3.return_value = LIST_URL

    DataIndexProcessor(
        job_id="job-1", subset_request=_request([ORIGINAL_NETCDF_KEY])
    ).process()

    find.assert_called_once()
    # One key, so the file name has no key in it.
    assert _uploaded_keys(aws) == ["job-1/SRS_Altimetry-file-list.zip"]


def test_unknown_key_sends_no_data_email(monkeypatch, aws):
    find = _found(monkeypatch, [FILE])

    DataIndexProcessor(job_id="job-1", subset_request=_request(["a.parquet"])).process()

    find.assert_not_called()
    aws.upload_file_to_s3.assert_not_called()
    assert aws.send_email.call_args.kwargs["subject"] == NO_DATA_EMAIL_SUBJECT


def test_no_title_names_the_list_after_the_uuid(monkeypatch, aws):
    _found(monkeypatch, [FILE])
    aws.upload_file_to_s3.return_value = LIST_URL
    request = replace(_request(), collection_title=None)

    DataIndexProcessor(job_id="job-1", subset_request=request).process()

    assert _uploaded_keys(aws) == [f"job-1/{UUID}-file-list.zip"]
