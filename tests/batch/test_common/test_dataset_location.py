"""Datasets hosted outside the AODN bucket must be read from where they live."""

from unittest.mock import MagicMock, patch

import pytest

from data_access_service.batch.common.dataset_location import (
    DatasetLocation,
    resolve_dataset_location,
)
from data_access_service.batch.common.dataset_scan import DatasetScanBase

CSIRO_DATASET = "uwy_csiro.parquet"
AODN_DATASET = "argo.parquet"

CSIRO_KEYS_RESPONSE = {
    "bucket": "dapprd-mnf",
    "remoteDirectory": "dapprd-mnf/000072626v001/",
    "endPointUrl": "https://s3.data.csiro.au",
    "accessKey": "csiro-key",
    "secretAccessKey": "csiro-secret",
}


def _mock_keys_response(overrides: dict | None = None):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {**CSIRO_KEYS_RESPONSE, **(overrides or {})}
    return response


class TestResolveDatasetLocation:
    def test_aodn_dataset_needs_no_keys(self):
        location = resolve_dataset_location(AODN_DATASET)

        assert location.bucket == "aodn-cloud-optimised"
        assert location.prefix == ""
        assert location.endpoint is None
        assert not location.is_external

    def test_csiro_dataset_resolves_to_its_own_bucket_and_endpoint(self):
        with patch(
            "data_access_service.models.co_data_source.csiro_access.requests.get",
            return_value=_mock_keys_response(),
        ):
            location = resolve_dataset_location(CSIRO_DATASET)

        assert location.bucket == "dapprd-mnf"
        assert location.prefix == "000072626v001/data/"
        # DuckDB's ENDPOINT wants the host on its own, without the scheme.
        assert location.endpoint == "s3.data.csiro.au"
        assert location.use_ssl is True
        assert location.is_external

    def test_missing_trailing_slash_does_not_glue_the_data_folder_on(self):
        with patch(
            "data_access_service.models.co_data_source.csiro_access.requests.get",
            return_value=_mock_keys_response(
                {"remoteDirectory": "dapprd-mnf/000072626v001"}
            ),
        ):
            location = resolve_dataset_location(CSIRO_DATASET)

        assert location.prefix == "000072626v001/data/"


class TestParquetGlob:
    def test_aodn_dataset_sits_at_the_bucket_root(self):
        location = DatasetLocation(bucket="aodn-cloud-optimised")

        assert (
            location.parquet_glob("argo.parquet")
            == "s3://aodn-cloud-optimised/argo.parquet/**/*.parquet"
        )

    def test_external_dataset_keeps_its_prefix(self):
        location = DatasetLocation(bucket="dapprd-mnf", prefix="000072626v001/data/")

        assert location.parquet_glob(CSIRO_DATASET) == (
            "s3://dapprd-mnf/000072626v001/data/uwy_csiro.parquet/**/*.parquet"
        )


class TestDatasetScanBaseUsesTheLocation:
    """The scan reads the dataset's real home, not the AODN bucket."""

    @pytest.fixture
    def pm_client(self, monkeypatch):
        client = MagicMock()
        monkeypatch.setattr(
            "data_access_service.batch.common.dataset_scan.PmTileDuckDBClient",
            lambda tuning=None: client,
        )
        return client

    def test_external_location_gets_its_own_s3_secret(self, pm_client, tmp_path):
        location = DatasetLocation(
            bucket="dapprd-mnf",
            prefix="000072626v001/data/",
            endpoint="s3.data.csiro.au",
            access_key="csiro-key",
            secret_access_key="csiro-secret",
        )

        scan = DatasetScanBase(
            uuid="a-uuid",
            dataset_name=CSIRO_DATASET,
            work_dir=str(tmp_path),
            api=MagicMock(),
            location=location,
        )

        assert scan.get_s3_uri() == (
            "s3://dapprd-mnf/000072626v001/data/uwy_csiro.parquet/**/*.parquet"
        )
        pm_client.create_s3_secret_with_keys.assert_called_once_with(
            bucket="dapprd-mnf",
            access_key="csiro-key",
            secret_access_key="csiro-secret",
            endpoint="s3.data.csiro.au",
            use_ssl=True,
        )

    def test_aodn_location_keeps_the_job_role_credentials(self, pm_client, tmp_path):
        scan = DatasetScanBase(
            uuid="a-uuid",
            dataset_name=AODN_DATASET,
            work_dir=str(tmp_path),
            api=MagicMock(),
            location=DatasetLocation(bucket="aodn-cloud-optimised"),
        )

        assert scan.get_s3_uri() == (
            "s3://aodn-cloud-optimised/argo.parquet/**/*.parquet"
        )
        pm_client.create_s3_secret_with_keys.assert_not_called()

    def test_location_is_resolved_when_the_caller_does_not_pass_one(
        self, pm_client, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            "data_access_service.batch.common.dataset_scan.resolve_dataset_location",
            lambda name: DatasetLocation(bucket="resolved-bucket"),
        )

        scan = DatasetScanBase(
            uuid="a-uuid",
            dataset_name=AODN_DATASET,
            work_dir=str(tmp_path),
            api=MagicMock(),
        )

        assert scan.get_s3_uri().startswith("s3://resolved-bucket/")


class TestCredentialsFollowEveryConnection:
    """A secret lives on one connection, so each new one needs it again."""

    EXTERNAL = DatasetLocation(
        bucket="dapprd-mnf",
        prefix="000072626v001/data/",
        endpoint="s3.data.csiro.au",
        access_key="csiro-key",
        secret_access_key="csiro-secret",
    )

    def _scan(self, location, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "data_access_service.batch.common.dataset_scan.PmTileDuckDBClient",
            lambda tuning=None: MagicMock(),
        )
        return DatasetScanBase(
            uuid="a-uuid",
            dataset_name=CSIRO_DATASET,
            work_dir=str(tmp_path),
            api=MagicMock(),
            location=location,
        )

    def test_a_rebuilt_client_gets_the_keys_again(self, tmp_path, monkeypatch):
        scan = self._scan(self.EXTERNAL, tmp_path, monkeypatch)
        later_client = MagicMock()

        scan.apply_location_credentials(later_client)

        later_client.create_s3_secret_with_keys.assert_called_once_with(
            bucket="dapprd-mnf",
            access_key="csiro-key",
            secret_access_key="csiro-secret",
            endpoint="s3.data.csiro.au",
            use_ssl=True,
        )

    def test_aodn_dataset_adds_nothing(self, tmp_path, monkeypatch):
        scan = self._scan(
            DatasetLocation(bucket="aodn-cloud-optimised"), tmp_path, monkeypatch
        )
        later_client = MagicMock()

        scan.apply_location_credentials(later_client)

        later_client.create_s3_secret_with_keys.assert_not_called()
