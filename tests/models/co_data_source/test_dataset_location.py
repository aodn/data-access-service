"""Datasets hosted outside the AODN bucket must resolve to where they live."""

from unittest.mock import MagicMock, patch

import pytest

from data_access_service.models.co_data_source.aodn_data_src import AodnDataSrc
from data_access_service.models.co_data_source.co_data_registory import (
    resolve_dataset_location,
)
from data_access_service.models.co_data_source.dataset_location import DatasetLocation

CSIRO_DATASET = "uwy_csiro.parquet"
AODN_DATASET = "argo.parquet"

# The Fedora PID stays on v1's number; the collection id does not.
CSIRO_COLLECTION_RESPONSE = {
    "dataCollectionId": 75215,
    "versionNumber": 4,
}

CSIRO_KEYS_RESPONSE = {
    "bucket": "dapprd-mnf",
    "remoteDirectory": "dapprd-mnf/000072626v004/",
    "endPointUrl": "https://s3.data.csiro.au",
    "accessKey": "csiro-key",
    "secretAccessKey": "csiro-secret",
}


def _mock_response(payload: dict, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    return response


def _patch_keys(
    overrides: dict | None = None, collection_overrides: dict | None = None
):
    """Patch both CSIRO calls: the collection lookup, then the key request."""
    return patch(
        "data_access_service.models.co_data_source.csiro_data_src.requests.get",
        side_effect=[
            _mock_response(
                {**CSIRO_COLLECTION_RESPONSE, **(collection_overrides or {})}
            ),
            _mock_response({**CSIRO_KEYS_RESPONSE, **(overrides or {})}),
        ],
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
        assert location.prefix == "000072626v004/data/"
        # DuckDB's ENDPOINT wants the host on its own, without the scheme.
        assert location.endpoint == "s3.data.csiro.au"
        assert location.use_ssl is True
        assert location.is_external

    def test_keys_are_requested_for_the_collection_the_pid_points_at(self):
        """The configured PID must not be used as the collection id itself."""
        with _patch_keys() as mock_get:
            resolve_dataset_location(CSIRO_DATASET)

        collection_url, keys_url = [call.args[0] for call in mock_get.call_args_list]
        assert collection_url.endswith("/collections/csiro:72626")
        assert keys_url.endswith("/collections/75215/files/s3")

    def test_missing_trailing_slash_does_not_glue_the_data_folder_on(self):
        with _patch_keys({"remoteDirectory": "dapprd-mnf/000072626v004"}):
            location = resolve_dataset_location(CSIRO_DATASET)

        assert location.prefix == "000072626v004/data/"

    def test_collection_lookup_failure_is_reported(self):
        failing = patch(
            "data_access_service.models.co_data_source.csiro_data_src.requests.get",
            return_value=_mock_response({}, status_code=503),
        )
        with failing, pytest.raises(Exception, match="collection id"):
            resolve_dataset_location(CSIRO_DATASET)

    def test_collection_without_an_id_is_reported(self):
        with _patch_keys(collection_overrides={"dataCollectionId": None}):
            with pytest.raises(Exception, match="dataCollectionId"):
                resolve_dataset_location(CSIRO_DATASET)

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
        location = DatasetLocation(bucket="dapprd-mnf", prefix="000072626v004/data/")

        assert location.parquet_glob(CSIRO_DATASET) == (
            "s3://dapprd-mnf/000072626v004/data/uwy_csiro.parquet/**/*.parquet"
        )
