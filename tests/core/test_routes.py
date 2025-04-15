import unittest
from unittest.mock import MagicMock, patch
from http import HTTPStatus
from data_access_service.server import create_app
from fastapi.testclient import TestClient


class TestRoutes(unittest.TestCase):
    @patch("data_access_service.server.API")
    def test_health_check_ready(self, MockAPI):
        mock_instance = MagicMock()
        mock_instance.get_api_status.return_value = True
        MockAPI.return_value = mock_instance

        app = create_app()
        client = TestClient(app)

        response = client.get("/api/v1/das/health")
        self.assertEqual(
            response.json(), {"status": "UP", "status_code": HTTPStatus.OK}
        )

    @patch("data_access_service.server.API")
    def test_health_check_not_ready(self, MockAPI):
        mock_instance = MagicMock()
        mock_instance.get_api_status.return_value = False
        MockAPI.return_value = mock_instance

        app = create_app()
        client = TestClient(app)

        response = client.get("/api/v1/das/health")
        self.assertEqual(
            response.json(),
            {"status": "STARTING", "status_code": HTTPStatus.SERVICE_UNAVAILABLE},
        )


if __name__ == "__main__":
    unittest.main()
