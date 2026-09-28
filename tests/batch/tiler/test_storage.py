"""S3 storage: path joining and JSON writes, with AWSHelper mocked."""

from unittest.mock import MagicMock

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


def test_upload_file_uploads_to_s3():
    helper = MagicMock()

    storage.upload_file(helper, "/tmp/slice.parquet", "s3://bucket/a/b.parquet")

    helper.upload_file_to_s3.assert_called_once_with(
        "/tmp/slice.parquet", "bucket", "a/b.parquet"
    )
