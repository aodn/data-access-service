"""DatasetScanBase must read the dataset's real home, not the AODN bucket."""

from unittest.mock import MagicMock

import pytest

from data_access_service.batch.common.dataset_scan import DatasetScanBase
from data_access_service.models.co_data_source.dataset_location import DatasetLocation

CSIRO_DATASET = "uwy_csiro.parquet"
AODN_DATASET = "argo.parquet"

EXTERNAL = DatasetLocation(
    bucket="dapprd-mnf",
    prefix="000072626v001/data/",
    endpoint="s3.data.csiro.au",
    access_key="csiro-key",
    secret_access_key="csiro-secret",
)
AODN = DatasetLocation(bucket="aodn-cloud-optimised")


@pytest.fixture
def pm_client(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(
        "data_access_service.batch.common.dataset_scan.PmTileDuckDBClient",
        lambda tuning=None: client,
    )
    return client


def _scan(location, tmp_path, dataset_name=CSIRO_DATASET):
    return DatasetScanBase(
        uuid="a-uuid",
        dataset_name=dataset_name,
        work_dir=str(tmp_path),
        api=MagicMock(),
        location=location,
    )


class TestUriComesFromTheLocation:
    def test_external_dataset_reads_from_its_own_bucket(self, pm_client, tmp_path):
        scan = _scan(EXTERNAL, tmp_path)

        assert scan.get_s3_uri() == (
            "s3://dapprd-mnf/000072626v001/data/uwy_csiro.parquet/**/*.parquet"
        )

    def test_aodn_dataset_is_unchanged(self, pm_client, tmp_path):
        scan = _scan(AODN, tmp_path, dataset_name=AODN_DATASET)

        assert (
            scan.get_s3_uri() == "s3://aodn-cloud-optimised/argo.parquet/**/*.parquet"
        )

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

    def test_the_first_client_gets_the_keys(self, pm_client, tmp_path):
        _scan(EXTERNAL, tmp_path)

        pm_client.create_s3_secret_with_keys.assert_called_once_with(
            bucket="dapprd-mnf",
            access_key="csiro-key",
            secret_access_key="csiro-secret",
            endpoint="s3.data.csiro.au",
            use_ssl=True,
        )

    def test_a_rebuilt_or_forked_client_gets_them_again(self, pm_client, tmp_path):
        scan = _scan(EXTERNAL, tmp_path)
        later_client = MagicMock()

        scan.apply_location_credentials(later_client)

        later_client.create_s3_secret_with_keys.assert_called_once_with(
            bucket="dapprd-mnf",
            access_key="csiro-key",
            secret_access_key="csiro-secret",
            endpoint="s3.data.csiro.au",
            use_ssl=True,
        )

    def test_aodn_dataset_adds_no_secret(self, pm_client, tmp_path):
        scan = _scan(AODN, tmp_path, dataset_name=AODN_DATASET)
        later_client = MagicMock()

        scan.apply_location_credentials(later_client)

        pm_client.create_s3_secret_with_keys.assert_not_called()
        later_client.create_s3_secret_with_keys.assert_not_called()
