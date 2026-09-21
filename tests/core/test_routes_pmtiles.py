"""PUT /pmtiles/{uuid} only submits an AWS Batch job - it never generates."""

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from data_access_service.config.config import Config
from data_access_service.core.routes import pmtiles as pmtiles_route
from data_access_service.server import app

UUID = "77dc5fca-8d1b-4b15-8a96-d7b0bbb1c3e6"
JOB_ID = "job-1234"

# The route resolved its config at import time; assert against that one.
config = pmtiles_route.config
# Sentinel so a test can send no header at all.
NO_API_KEY = object()


@pytest.fixture
def api_instance():
    """The app state the route reads, without starting the real lifespan."""
    instance = MagicMock()
    instance.get_api_status.return_value = True
    instance.get_mapped_meta_data.return_value = {
        "a.parquet": MagicMock(),
        "b.zarr": MagicMock(),
    }
    app.state.api_instance = instance
    return instance


@pytest.fixture
def submit_a_job(monkeypatch):
    stub = MagicMock(return_value=JOB_ID)
    monkeypatch.setattr(pmtiles_route.aws, "submit_a_job", stub)
    return stub


def _put(uuid: str = UUID, api_key=None):
    # The api key is only known once a test is running (the profile is then
    # TESTING), so it cannot be a module level default.
    if api_key is None:
        api_key = Config.get_config().get_api_key()
    headers = {} if api_key is NO_API_KEY else {"X-API-Key": api_key}
    return TestClient(app).put(f"/api/v1/das/pmtiles/{uuid}", headers=headers)


class TestJobSubmission:
    def test_submits_a_batch_job_and_returns_the_job_id(
        self, api_instance, submit_a_job
    ):
        response = _put()

        assert response.status_code == HTTPStatus.ACCEPTED
        body = response.json()
        assert body["job_id"] == JOB_ID
        assert body["uuid"] == UUID
        assert body["job_queue"] == config.get_job_queue_name()
        assert JOB_ID in body["message"]

        submit_a_job.assert_called_once()
        kwargs = submit_a_job.call_args.kwargs
        assert kwargs["parameters"] == {
            "type": "generate-pmtiles-for-parquet",
            "uuid": UUID,
        }
        assert kwargs["job_queue"] == config.get_job_queue_name()
        assert kwargs["job_definition"] == config.get_job_definition_name()

    def test_job_name_is_built_from_the_uuid(self, api_instance, submit_a_job):
        _put()

        assert submit_a_job.call_args.kwargs["job_name"] == f"pmtiles-{UUID}"

    def test_job_name_has_no_characters_batch_rejects(self):
        assert pmtiles_route._job_name("a.b c/d") == "pmtiles-a-b-c-d"

    def test_long_names_are_truncated(self):
        assert len(pmtiles_route._job_name("u" * 200)) == 128


class TestRejectedRequests:
    def test_unknown_uuid_is_not_submitted(self, api_instance, submit_a_job):
        # What get_mapped_meta_data returns for a uuid it does not know
        api_instance.get_mapped_meta_data.return_value = {"not_exist": MagicMock()}

        response = _put()

        assert response.status_code == HTTPStatus.NOT_FOUND
        submit_a_job.assert_not_called()

    def test_uuid_without_parquet_is_not_submitted(self, api_instance, submit_a_job):
        api_instance.get_mapped_meta_data.return_value = {"b.zarr": MagicMock()}

        response = _put()

        assert response.status_code == HTTPStatus.NOT_FOUND
        submit_a_job.assert_not_called()

    def test_missing_api_key_is_not_submitted(self, api_instance, submit_a_job):
        response = _put(api_key=NO_API_KEY)

        assert response.status_code == HTTPStatus.UNAUTHORIZED
        submit_a_job.assert_not_called()

    def test_wrong_api_key_is_not_submitted(self, api_instance, submit_a_job):
        response = _put(api_key="not-the-key")

        assert response.status_code == HTTPStatus.UNAUTHORIZED
        submit_a_job.assert_not_called()

    def test_api_still_initializing_is_not_submitted(self, api_instance, submit_a_job):
        api_instance.get_api_status.return_value = False

        response = _put()

        assert response.status_code == HTTPStatus.SERVICE_UNAVAILABLE
        submit_a_job.assert_not_called()
