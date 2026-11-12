"""A failed preparation child leaves a marker; the collector turns any marker
into one PreparationFailedError (and so one error email)."""

from unittest.mock import MagicMock

import pytest

from data_access_service.batch.subsetting.helpers import preparation_failure
from data_access_service.batch.subsetting.helpers.preparation_failure import (
    PreparationFailedError,
    preparation_failure_prefix,
    raise_if_preparation_failed,
    record_preparation_failure,
)
from data_access_service.batch.subsetting.tasks import parquet_collector
from data_access_service.models.subset_request import SubsetRequest


@pytest.fixture
def aws(monkeypatch):
    config = MagicMock()
    config.get_subsetting_bucket_name.return_value = "bucket"
    config.get_s3_temp_folder_name.side_effect = lambda job_id: f"{job_id}/temp/"
    aws = MagicMock()
    for module in (preparation_failure, parquet_collector):
        monkeypatch.setattr(module.Config, "get_config", lambda: config)
        monkeypatch.setattr(module, "init_log", lambda config: MagicMock())
        monkeypatch.setattr(module, "AWSHelper", lambda: aws)
    return aws


def test_marker_is_outside_the_collector_temp_folder():
    assert preparation_failure_prefix("job") == "job/errors/"
    assert not preparation_failure_prefix("job").startswith("job/temp/")


def test_record_writes_marker_per_child(aws):
    assert record_preparation_failure("job", "3", ValueError("boom")) is True
    file_obj, bucket, key = aws.upload_fileobj_to_s3.call_args.args
    assert (bucket, key) == ("bucket", "job/errors/3.txt")
    assert b"ValueError" in file_obj.getvalue()


def test_record_returns_false_when_upload_fails(aws):
    aws.upload_fileobj_to_s3.side_effect = RuntimeError("S3 down")
    assert record_preparation_failure("job", "3", ValueError("boom")) is False


def test_raise_if_preparation_failed():
    aws = MagicMock()
    aws.get_s3_keys.return_value = []
    raise_if_preparation_failed(aws, "bucket", "job")
    aws.get_s3_keys.assert_called_once_with("bucket", "job/errors/")

    aws.get_s3_keys.return_value = ["job/errors/0.txt", "job/errors/2.txt"]
    with pytest.raises(PreparationFailedError, match="2 data preparation job"):
        raise_if_preparation_failed(aws, "bucket", "job")


def _request():
    return SubsetRequest(
        uuid="test-uuid",
        keys=["data.parquet"],
        start_date="2020-01-01",
        end_date="2020-01-31",
        recipient="user@example.com",
        output_format="csv",
    )


def test_collector_stops_before_zipping_when_any_child_failed(aws):
    aws.get_s3_keys.return_value = ["job/errors/1.txt"]
    with pytest.raises(PreparationFailedError):
        parquet_collector.collect_parquet_files("job", _request())
    aws.list_s3_folders.assert_not_called()
    aws.write_csv_to_s3.assert_not_called()
    aws.send_email.assert_not_called()  # collect_data sends the error email


def test_collector_without_markers_sends_success_email(aws):
    aws.get_s3_keys.return_value = []
    aws.list_s3_folders.return_value = ["data.parquet"]
    aws.write_csv_to_s3.return_value = "https://bucket/job/result.zip"
    no_such_key = type("NoSuchKey", (Exception,), {})
    aws.s3.exceptions.NoSuchKey = no_such_key
    aws.s3.get_object.side_effect = no_such_key()
    parquet_collector.collect_parquet_files("job", _request())
    aws.write_csv_to_s3.assert_called_once()
    aws.send_email.assert_called_once()
    assert (
        "https://bucket/job/result.zip" in aws.send_email.call_args.kwargs["html_body"]
    )
