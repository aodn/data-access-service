"""Shared fixtures for the co_datasource tests."""

import pytest
from tenacity import wait_none

from data_access_service.models.co_datasource.csiro import csiro_data_src


@pytest.fixture(autouse=True)
def no_csiro_retry_wait(monkeypatch):
    """Keep the CSIRO retries, drop their backoff, so failure tests stay fast."""
    monkeypatch.setattr(csiro_data_src._call_csiro_api.retry, "wait", wait_none())
