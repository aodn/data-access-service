from unittest.mock import MagicMock, patch

import duckdb
import pytest

from data_access_service import Config
from data_access_service.config.config import IntTestConfig
from data_access_service.core.duckdbclient import DuckDBClient


class _Client(DuckDBClient):
    """A plain in-memory connection with httpfs loaded."""

    def __init__(self):
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


def test_secret_uses_credential_chain_with_auto_refresh(monkeypatch, not_int_test):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-2")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    client = _Client()

    client.create_s3_secret("my-bucket")

    name, provider, scope = client.execute(
        "SELECT name, provider, scope FROM duckdb_secrets()"
    ).fetchone()
    assert name == "my-bucket_s3"
    assert provider == "credential_chain"
    assert scope == ["s3://my-bucket"]
    client.close()


def test_no_secret_without_credentials(not_int_test):
    client = MagicMock(spec=DuckDBClient)
    session = MagicMock()
    session.get_credentials.return_value = None
    with patch("boto3.Session", return_value=session):
        DuckDBClient.create_s3_secret(client, "my-bucket")
    client.execute.assert_not_called()


def test_no_secret_in_int_tests():
    client = MagicMock(spec=DuckDBClient)
    with patch.object(Config, "get_config", return_value=MagicMock(spec=IntTestConfig)):
        DuckDBClient.create_s3_secret(client, "my-bucket")
    client.execute.assert_not_called()
