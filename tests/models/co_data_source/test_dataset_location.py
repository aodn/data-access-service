"""Datasets hosted outside the AODN bucket must resolve to where they live."""

from unittest.mock import MagicMock, patch

from data_access_service.models.co_data_source.aodn_data_src import AodnDataSrc
from data_access_service.models.co_data_source.co_data_registory import (
    resolve_dataset_location,
)
from data_access_service.models.co_data_source.dataset_location import DatasetLocation

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


def _patch_keys(overrides: dict | None = None):
    return patch(
        "data_access_service.models.co_data_source.csiro_data_src.requests.get",
        return_value=_mock_keys_response(overrides),
    )


class TestResolveDatasetLocation:
    def test_aodn_dataset_needs_no_keys(self):
        location = resolve_dataset_location(AODN_DATASET)

        assert location.bucket == "aodn-cloud-optimised"
        assert location.prefix == ""
        assert location.endpoint is None
        assert not location.is_external

    def test_csiro_dataset_resolves_to_its_own_bucket_and_endpoint(self):
        with _patch_keys():
            location = resolve_dataset_location(CSIRO_DATASET)

        assert location.bucket == "dapprd-mnf"
        assert location.prefix == "000072626v001/data/"
        # DuckDB's ENDPOINT wants the host on its own, without the scheme.
        assert location.endpoint == "s3.data.csiro.au"
        assert location.use_ssl is True
        assert location.is_external

    def test_missing_trailing_slash_does_not_glue_the_data_folder_on(self):
        with _patch_keys({"remoteDirectory": "dapprd-mnf/000072626v001"}):
            location = resolve_dataset_location(CSIRO_DATASET)

        assert location.prefix == "000072626v001/data/"

    def test_a_source_that_does_not_claim_the_dataset_is_skipped(self, monkeypatch):
        """locate_dataset returning None must fall through to the next source."""

        class _Declines:
            @classmethod
            def locate_dataset(cls, name):
                return None

        class _Claims:
            @classmethod
            def locate_dataset(cls, name):
                return DatasetLocation(bucket="second-provider")

        monkeypatch.setattr(
            "data_access_service.models.co_data_source.co_data_registory._DATA_SOURCES",
            [_Declines, _Claims],
        )

        assert resolve_dataset_location(AODN_DATASET).bucket == "second-provider"

    def test_aodn_source_never_claims(self):
        """AodnDataSrc.locate_dataset must not list the bucket to answer."""
        assert AodnDataSrc.locate_dataset(AODN_DATASET) is None


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
