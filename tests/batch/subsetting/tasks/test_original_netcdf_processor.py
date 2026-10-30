"""OriginalNetcdfProcessor: query result -> zipped CSV file list + email."""

import csv
import io
import zipfile
from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_access_service.batch.subsetting.tasks import original_netcdf_processor
from data_access_service.batch.subsetting.tasks.original_netcdf_processor import (
    FILE_LIST_COLUMNS,
    OriginalNetcdfProcessor,
)
from data_access_service.models.data_index_types import OriginalNetcdf
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.no_data_email import (
    NO_DATA_EMAIL_SUBJECT,
)

UUID = "uuid-1"
LIST_URL = "https://bucket.s3.ap-southeast-2.amazonaws.com/job-1/list.zip"

FILE = OriginalNetcdf(
    bucket="imos-data",
    key="IMOS/SRS/a file.nc",
    time_coverage_start=pd.Timestamp("2020-01-01", tz="UTC"),
    time_coverage_end=pd.Timestamp("2020-01-02", tz="UTC"),
    lat_min=-40.0,
    lat_max=-30.0,
    lon_min=140.0,
    lon_max=150.0,
)


def _request():
    return SubsetRequest(
        uuid=UUID,
        keys=["*"],
        start_date="2020-01-01",
        end_date="2020-01-31",
        recipient="user@example.com",
        output_format="netcdf",
        collection_title="SRS Altimetry",
    )


@pytest.fixture
def aws(monkeypatch):
    aws = MagicMock()
    monkeypatch.setattr(original_netcdf_processor, "AWSHelper", lambda: aws)
    # No S3 Table in tests.
    monkeypatch.setattr(original_netcdf_processor, "DataIndexDuckDBClient", MagicMock())
    return aws


def _found(monkeypatch, files):
    find = MagicMock(return_value=files)
    monkeypatch.setattr(original_netcdf_processor, "find_original_netcdfs", find)
    return find


def test_no_files_sends_no_data_email(monkeypatch, aws):
    _found(monkeypatch, [])

    OriginalNetcdfProcessor(job_id="job-1", subset_request=_request()).process()

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

    OriginalNetcdfProcessor(job_id="job-1", subset_request=_request()).process()

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
