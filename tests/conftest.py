import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def mock_aws_client():
    print("Mocking AWSClient for tests")
    with patch("data_access_service.core.AWSClient.AWSClient") as mock:
        yield mock
