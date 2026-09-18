"""S3 storage: path handling, JSON round-trips.

S3 calls go through a monkeypatched AWSHelper — no real S3/moto needed, since
this module's own job is composing the right bucket/key and calling AWSHelper
correctly, not the AWS wire protocol itself.
"""

from unittest.mock import MagicMock

from data_access_service.batch.tiler import storage


def test_join_s3_uri():
    assert storage.join("s3://bucket/prefix", "foo", "bar.parquet") == (
        "s3://bucket/prefix/foo/bar.parquet"
    )


def test_join_strips_trailing_slash_on_base():
    assert storage.join("s3://bucket/prefix/", "foo") == "s3://bucket/prefix/foo"


def test_split_s3():
    assert storage._split_s3("s3://bucket/a/b/c.json") == ("bucket", "a/b/c.json")


# --- S3 read/write (AWSHelper mocked) ------------------------------------


def test_write_json_uploads_fileobj_to_s3(monkeypatch):
    helper = MagicMock()
    monkeypatch.setattr(storage, "AWSHelper", lambda: helper)

    storage.write_json("s3://bucket/a/metadata.json", {"a": 1})

    helper.upload_fileobj_to_s3.assert_called_once()
    file_obj, bucket, key = helper.upload_fileobj_to_s3.call_args[0]
    assert bucket == "bucket"
    assert key == "a/metadata.json"
    assert file_obj.read() == b'{"a": 1}'


def test_read_json_fetches_from_s3(monkeypatch):
    helper = MagicMock()
    helper.get_s3_object.return_value = b'{"a": 1}'
    monkeypatch.setattr(storage, "AWSHelper", lambda: helper)

    result = storage.read_json("s3://bucket/a/metadata.json")

    helper.get_s3_object.assert_called_once_with("bucket", "a/metadata.json")
    assert result == {"a": 1}


def test_read_json_returns_none_when_s3_object_missing(monkeypatch):
    helper = MagicMock()
    helper.get_s3_object.return_value = None
    monkeypatch.setattr(storage, "AWSHelper", lambda: helper)

    assert storage.read_json("s3://bucket/missing.json") is None
