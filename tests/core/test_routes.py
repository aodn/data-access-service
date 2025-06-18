import unittest
from unittest.mock import MagicMock, patch
from http import HTTPStatus
from fastapi.testclient import TestClient
from data_access_service.server import app


class TestRoutes(unittest.TestCase):
    @patch("data_access_service.tasks.sync_aws_batch_configs.sync_aws_batch_configs")
    @patch("data_access_service.server.API")
    def test_health_check_ready(self, mock_api, mock_sync):
        mock_instance = MagicMock()
        mock_instance.get_api_status.return_value = True
        mock_api.return_value = mock_instance
        mock_sync = MagicMock()
        mock_sync.return_value = None

        with TestClient(app) as client:
            response = client.get("/api/v1/das/health")
            self.assertEqual(
                response.json(), {"status": "UP", "status_code": HTTPStatus.OK}
            )

    @patch("data_access_service.tasks.sync_aws_batch_configs.sync_aws_batch_configs")
    @patch("data_access_service.server.API")
    def test_health_check_not_ready(self, mock_api, mock_sync):
        mock_instance = MagicMock()
        mock_instance.get_api_status.return_value = False
        mock_api.return_value = mock_instance
        mock_sync = MagicMock()
        mock_sync.return_value = None

        with TestClient(app) as client:
            response = client.get("/api/v1/das/health")
            self.assertEqual(
                response.json(),
                {"status": "STARTING", "status_code": HTTPStatus.OK},
            )


if __name__ == "__main__":
    unittest.main()
