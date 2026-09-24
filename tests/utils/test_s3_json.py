"""S3 JSON reads through a monkeypatched AWSHelper."""

from unittest.mock import MagicMock

import pytest

from data_access_service.utils import s3_json


def test_split_s3():
    assert s3_json.split_s3("s3://bucket/a/b/c.json") == ("bucket", "a/b/c.json")


def _mock_helper(monkeypatch, body):
    helper = MagicMock()
    helper.get_s3_object.return_value = body
    monkeypatch.setattr(s3_json, "AWSHelper", lambda: helper)
    return helper


def test_read_json_fetches_from_s3(monkeypatch):
    helper = _mock_helper(monkeypatch, b'{"a": 1}')

    assert s3_json.read_json("s3://bucket/a/metadata.json") == {"a": 1}
    helper.get_s3_object.assert_called_once_with("bucket", "a/metadata.json")


def test_read_json_raises_when_required_and_missing(monkeypatch):
    _mock_helper(monkeypatch, None)

    with pytest.raises(FileNotFoundError):
        s3_json.read_json("s3://bucket/missing.json")


def test_read_json_returns_none_when_not_required_and_missing(monkeypatch):
    _mock_helper(monkeypatch, None)

    assert s3_json.read_json("s3://bucket/missing.json", required=False) is None
