import pytest
import asyncio
from unittest.mock import MagicMock
import pandas as pd
from data_access_service.utils.routes_helper import fetch_data


@pytest.mark.asyncio
async def test_fetch_data_returns_nothing_when_get_dataset_none():
    """
    This test is ued when the get_dataset method returns None, but the fetch_data
    generator was not exhausted before. result from the fetch_data should be empty
    without any error.
    """

    class DummyAPI:
        def get_dataset(self, *args, **kwargs):
            return None

    api_instance = DummyAPI()
    # Provide dummy arguments for required parameters
    uuid = "dummy"
    key = "dummy"
    start_date = pd.Timestamp("2020-01-01")
    end_date = pd.Timestamp("2020-01-02")
    start_depth = None
    end_depth = None
    columns = []

    results = [
        item
        async for item in fetch_data(
            api_instance,
            uuid,
            key,
            start_date,
            end_date,
            start_depth,
            end_depth,
            columns,
        )
    ]
    assert results == []
