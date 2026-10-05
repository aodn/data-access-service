from unittest.mock import MagicMock, patch

import duckdb
import pytest

from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.duckdbclient import DuckDBClient


class _Client(DuckDBClient):
    """A plain in-memory connection with httpfs loaded."""

    def __init__(self):
        super().__init__()
        self._con = duckdb.connect()
        self._con.execute("INSTALL httpfs; LOAD httpfs;")

    def get_instance(self):
        return self._con

    def execute(self, sql, params=None):
        return self._con.execute(sql)

    def close(self):
        self._con.close()


@pytest.fixture
def not_int_test():
    with patch.object(Config, "get_config", return_value=MagicMock()):
        yield


def test_secret_uses_credential_chain_without_in_query_refresh(
    monkeypatch, not_int_test
):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-2")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    client = _Client()

    client.create_s3_secret("my-bucket")

    name, provider, scope, secret_string = client.execute(
        "SELECT name, provider, scope, secret_string FROM duckdb_secrets()"
    ).fetchone()
    assert name == "my-bucket_s3"
    assert provider == "credential_chain"
    assert scope == ["s3://my-bucket"]
    assert "refresh=auto" not in secret_string

    client.create_s3_secret("my-bucket")
    rows = client.execute(
        "SELECT name FROM duckdb_secrets() WHERE name = 'my-bucket_s3'"
    ).fetchall()
    assert rows == [("my-bucket_s3",)]
    client.close()


def test_no_secret_without_credentials(not_int_test):
    client = MagicMock(spec=DuckDBClient)
    session = MagicMock()
    session.get_credentials.return_value = None
    with patch("boto3.Session", return_value=session):
        DuckDBClient.create_s3_secret(client, "my-bucket")
    client.execute.assert_not_called()


def test_expired_token_replaces_secret_from_boto_and_retries(monkeypatch, not_int_test):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-2")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    client = _Client()
    client.create_s3_secret("my-bucket")
    calls = {"n": 0}

    def read():
        calls["n"] += 1
        if calls["n"] == 1:
            raise duckdb.HTTPException(
                "HTTP 400 ExpiredToken: The provided token has expired."
            )
        return "ok"

    frozen = MagicMock(
        access_key="AKIAFRESH", secret_key="new-secret", token="new-token"
    )
    session = MagicMock()
    session.region_name = "ap-southeast-2"
    session.get_credentials.return_value.get_frozen_credentials.return_value = frozen
    with patch("boto3.Session", return_value=session):
        assert client._call_refreshing_s3(read) == "ok"

    assert calls["n"] == 2
    provider = client.execute(
        "SELECT provider FROM duckdb_secrets() WHERE name = 'my-bucket_s3'"
    ).fetchone()[0]
    assert provider != "credential_chain"
    client.close()


def test_refresh_runs_once_per_generation():
    client = _Client()
    client._s3_chain_secrets.add("my-bucket")
    client._s3_secret_generation = 1
    with patch.object(client, "_replace_chain_secrets_from_boto") as replace:
        client._refresh_expired_s3_secrets(0)
    replace.assert_not_called()
    client.close()


def test_http_403_replaces_secret_once_and_retries():
    client = _Client()
    client._s3_chain_secrets.add("my-bucket")
    calls = {"n": 0}

    def read():
        calls["n"] += 1
        if calls["n"] == 1:
            raise duckdb.HTTPException("HTTP Error: HTTP 403 Forbidden")
        return "ok"

    with patch.object(client, "_replace_chain_secrets_from_boto") as replace:
        assert client._call_refreshing_s3(read) == "ok"
    assert calls["n"] == 2
    replace.assert_called_once()
    client.close()


def test_other_http_errors_are_not_retried():
    client = _Client()

    def read():
        raise duckdb.HTTPException("HTTP 404 Not Found")

    with pytest.raises(duckdb.HTTPException):
        client._call_refreshing_s3(read)
    client.close()


def test_no_secret_in_int_tests():
    client = MagicMock(spec=DuckDBClient)
    with patch.object(Config, "get_config", return_value=MagicMock(spec=IntTestConfig)):
        DuckDBClient.create_s3_secret(client, "my-bucket")
    client.execute.assert_not_called()
