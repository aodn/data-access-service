"""S3 storage: path joining and JSON writes, with AWSHelper mocked."""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from data_access_service.batch.tiler import storage


def test_join_s3_uri():
    assert storage.join("s3://bucket/prefix", "foo", "bar.parquet") == (
        "s3://bucket/prefix/foo/bar.parquet"
    )


def test_join_strips_trailing_slash_on_base():
    assert storage.join("s3://bucket/prefix/", "foo") == "s3://bucket/prefix/foo"


# --- S3 read/write (AWSHelper mocked) ------------------------------------


def test_write_json_uploads_fileobj_to_s3():
    helper = MagicMock()

    storage.write_json(helper, "s3://bucket/a/metadata.json", {"a": 1})

    helper.upload_fileobj_to_s3.assert_called_once()
    file_obj, bucket, key = helper.upload_fileobj_to_s3.call_args[0]
    assert bucket == "bucket"
    assert key == "a/metadata.json"
    assert file_obj.read() == b'{"a": 1}'


def test_upload_file_uploads_to_s3_on_one_connection():
    helper = MagicMock()

    storage.upload_file(helper, "/tmp/slice.parquet", "s3://bucket/a/b.parquet")

    helper.s3.upload_file.assert_called_once()
    args, kwargs = helper.s3.upload_file.call_args
    assert args == ("/tmp/slice.parquet", "bucket", "a/b.parquet")
    assert kwargs["Config"].use_threads is False


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "PutObject")


def test_write_json_if_unchanged_requires_a_new_file_without_an_etag():
    helper = MagicMock()

    storage.write_json_if_unchanged(helper, "s3://bucket/root.json", {"a": 1}, None)

    kwargs = helper.s3.put_object.call_args.kwargs
    assert kwargs["IfNoneMatch"] == "*"
    assert "IfMatch" not in kwargs
    assert kwargs["Body"] == b'{"a": 1}'


def test_write_json_if_unchanged_matches_the_etag():
    helper = MagicMock()

    storage.write_json_if_unchanged(helper, "s3://bucket/root.json", {}, '"abc"')

    kwargs = helper.s3.put_object.call_args.kwargs
    assert kwargs["IfMatch"] == '"abc"'
    assert "IfNoneMatch" not in kwargs


def test_write_json_if_unchanged_raises_conflict_on_precondition_failure():
    helper = MagicMock()
    helper.s3.put_object.side_effect = _client_error("PreconditionFailed")

    with pytest.raises(storage.WriteConflict):
        storage.write_json_if_unchanged(helper, "s3://bucket/root.json", {}, "e")


def test_write_json_if_unchanged_reraises_other_errors():
    helper = MagicMock()
    helper.s3.put_object.side_effect = _client_error("AccessDenied")

    with pytest.raises(ClientError):
        storage.write_json_if_unchanged(helper, "s3://bucket/root.json", {}, "e")
