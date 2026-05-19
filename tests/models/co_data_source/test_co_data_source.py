"""
Unit tests for the CO_data_source package:
  - abstract_data_src.py  (via a concrete stub)
  - aodn_data_src.py
  - csiro_data_src.py
  - co_data_registory.py
"""

from unittest.mock import MagicMock, patch

import pytest

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError
from data_access_service.models.co_data_source.abstract_data_src import (
    AbstractDataSrc,
    AODN,
    CSIRO,
)
from data_access_service.models.co_data_source.aodn_data_src import AodnDataSrc
from data_access_service.models.co_data_source.csiro_data_src import CsiroDataSrc
from data_access_service.models.co_data_source.co_data_registory import CODataRegistry

# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

KNOWN_DATASET = "some_dataset.parquet"
UNKNOWN_DATASET = "does_not_exist.parquet"


def _make_mock_get_aodn(catalog: dict | None = None) -> MagicMock:
    """Return a mock GetAodn whose get_dataset() returns a mock DataSource."""
    mock_aodn = MagicMock()
    mock_dataset = MagicMock()
    mock_aodn.get_dataset.return_value = mock_dataset

    mock_metadata = MagicMock()
    mock_metadata.catalog = catalog or {KNOWN_DATASET: {}}
    mock_aodn.get_metadata.return_value = mock_metadata
    return mock_aodn


# ---------------------------------------------------------------------------
# AbstractDataSrc (tested through a minimal concrete stub)
# ---------------------------------------------------------------------------


class _ConcreteDataSrc(AbstractDataSrc):
    """Minimal concrete implementation used only for testing AbstractDataSrc logic."""

    def __init__(self, catalog: dict, data_src: MagicMock):
        self._catalog = catalog
        self._data_src = data_src

    def get_metadata(self):
        pass

    def get_metadata_catalog(self) -> dict:
        return self._catalog

    def get_dataset(self, dataset_name_with_ext: str):
        return super().get_dataset(dataset_name_with_ext=dataset_name_with_ext)

    def get_name(self) -> str:
        return "test_src"

    def get_data_src(self):
        return self._data_src


class TestAbstractDataSrc:
    def test_get_dataset_raises_when_not_in_catalog(self):
        src = _ConcreteDataSrc(catalog={KNOWN_DATASET: {}}, data_src=MagicMock())
        with pytest.raises(DatasetNotFoundError):
            src.get_dataset(UNKNOWN_DATASET)

    def test_get_dataset_delegates_to_data_src_when_in_catalog(self):
        mock_data_src = MagicMock()
        expected = MagicMock()
        mock_data_src.get_dataset.return_value = expected

        src = _ConcreteDataSrc(catalog={KNOWN_DATASET: {}}, data_src=mock_data_src)
        result = src.get_dataset(KNOWN_DATASET)

        mock_data_src.get_dataset.assert_called_once_with(KNOWN_DATASET)
        assert result is expected

    def test_dataset_not_found_error_carries_dataset_name(self):
        src = _ConcreteDataSrc(catalog={}, data_src=MagicMock())
        with pytest.raises(DatasetNotFoundError) as exc_info:
            src.get_dataset(UNKNOWN_DATASET)
        assert exc_info.value.dataset_name == UNKNOWN_DATASET


# ---------------------------------------------------------------------------
# AodnDataSrc
# ---------------------------------------------------------------------------


class TestAodnDataSrc:
    def _make_src(self) -> tuple[AodnDataSrc, MagicMock]:
        mock_aodn = _make_mock_get_aodn(catalog={KNOWN_DATASET: {"info": 1}})
        with patch(
            "data_access_service.models.CO_data_source.aodn_data_src.GetAodn",
            return_value=mock_aodn,
        ):
            src = AodnDataSrc()
        return src, mock_aodn

    def test_get_name_returns_aodn(self):
        src, _ = self._make_src()
        assert src.get_name() == AODN

    def test_get_metadata_catalog_returns_catalog_from_data_src(self):
        src, mock_aodn = self._make_src()
        assert src.get_metadata_catalog() == {KNOWN_DATASET: {"info": 1}}

    def test_get_metadata_returns_metadata_object(self):
        src, mock_aodn = self._make_src()
        metadata = src.get_metadata()
        assert metadata is mock_aodn.get_metadata.return_value

    def test_get_dataset_returns_dataset_for_known_key(self):
        src, mock_aodn = self._make_src()
        result = src.get_dataset(KNOWN_DATASET)
        mock_aodn.get_dataset.assert_called_once_with(KNOWN_DATASET)
        assert result is mock_aodn.get_dataset.return_value

    def test_get_dataset_raises_for_unknown_key(self):
        src, _ = self._make_src()
        with pytest.raises(DatasetNotFoundError):
            src.get_dataset(UNKNOWN_DATASET)


# ---------------------------------------------------------------------------
# CsiroDataSrc
# ---------------------------------------------------------------------------

_CSIRO_API_RESPONSE = {
    "accessKey": "AKID",
    "secretAccessKey": "SECRET",
    "endPointUrl": "https://s3.example.com",
    "bucket": "dapprd-mnf",
    "remoteDirectory": "dapprd-mnf/000072626v001",
}

_CSIRO_DATASET_METADATA = {
    "global_attributes": {
        "title": "MNF Underway",
    }
}


def _patch_csiro(api_response=None, metadata=None):
    """Returns a context-manager patch combo for CsiroDataSrc init dependencies."""
    if api_response is None:
        api_response = _CSIRO_API_RESPONSE
    if metadata is None:
        metadata = _CSIRO_DATASET_METADATA

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = api_response

    mock_dataset = MagicMock()
    mock_dataset.get_metadata.return_value = dict(metadata)  # mutable copy

    mock_aodn = MagicMock()
    mock_aodn.get_dataset.return_value = mock_dataset

    requests_patch = patch(
        "data_access_service.models.CO_data_source.csiro_data_src.requests.get",
        return_value=mock_response,
    )
    get_aodn_patch = patch(
        "data_access_service.models.CO_data_source.csiro_data_src.GetAodn",
        return_value=mock_aodn,
    )
    return requests_patch, get_aodn_patch, mock_aodn


class TestCsiroDataSrc:
    def _make_src(self, api_response=None, metadata=None):
        r_patch, a_patch, mock_aodn = _patch_csiro(api_response, metadata)
        with r_patch, a_patch:
            src = CsiroDataSrc()
        return src, mock_aodn

    def test_get_name_returns_csiro(self):
        src, _ = self._make_src()
        assert src.get_name() == CSIRO

    def test_get_metadata_raises_not_implemented(self):
        src, _ = self._make_src()
        with pytest.raises(NotImplementedError):
            src.get_metadata()

    def test_get_dataset_raises_for_unknown_dataset(self):
        src, _ = self._make_src()
        with pytest.raises(DatasetNotFoundError):
            src.get_dataset(UNKNOWN_DATASET)

    def test_get_dataset_returns_dataset_for_known_name(self):
        src, mock_aodn = self._make_src()
        result = src.get_dataset(CsiroDataSrc.THE_ONLY_DATASET_NAME)
        assert result is mock_aodn.get_dataset.return_value

    def test_init_raises_when_api_returns_non_200(self):
        mock_response = MagicMock()
        mock_response.status_code = 403

        with patch(
            "data_access_service.models.CO_data_source.csiro_data_src.requests.get",
            return_value=mock_response,
        ):
            with pytest.raises(Exception, match="Failed to get keys from CSIRO"):
                CsiroDataSrc()

    def test_init_raises_when_remote_directory_format_unexpected(self):
        bad_response = dict(_CSIRO_API_RESPONSE)
        bad_response["remoteDirectory"] = "wrong_bucket/path"

        r_patch, a_patch, _ = _patch_csiro(api_response=bad_response)
        with r_patch, a_patch:
            with pytest.raises(Exception, match="Unexpected remote directory format"):
                CsiroDataSrc()

    def test_metadata_catalog_injects_uuid_when_missing(self):
        metadata_without_uuid = {
            "global_attributes": {
                "title": "MNF Underway",
                # no metadata_uuid
            }
        }
        src, _ = self._make_src(metadata=metadata_without_uuid)
        catalog = src.get_metadata_catalog()
        uuid = catalog[CsiroDataSrc.THE_ONLY_DATASET_NAME]["global_attributes"][
            "metadata_uuid"
        ]
        assert uuid == "154a59da-b88a-4231-97df-c0407a6f0ec4"

    def test_metadata_catalog_preserves_existing_uuid(self):
        metadata_with_uuid = {
            "global_attributes": {
                "metadata_uuid": "existing-uuid",
            }
        }
        src, _ = self._make_src(metadata=metadata_with_uuid)
        catalog = src.get_metadata_catalog()
        assert (
            catalog[CsiroDataSrc.THE_ONLY_DATASET_NAME]["global_attributes"][
                "metadata_uuid"
            ]
            == "existing-uuid"
        )

    def test_init_raises_when_metadata_is_not_dict(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = _CSIRO_API_RESPONSE

        mock_dataset = MagicMock()
        mock_dataset.get_metadata.return_value = "not-a-dict"

        mock_aodn = MagicMock()
        mock_aodn.get_dataset.return_value = mock_dataset

        with patch(
            "data_access_service.models.CO_data_source.csiro_data_src.requests.get",
            return_value=mock_response,
        ), patch(
            "data_access_service.models.CO_data_source.csiro_data_src.GetAodn",
            return_value=mock_aodn,
        ):
            with pytest.raises(
                Exception, match="Unexpected metadata format for CSIRO dataset"
            ):
                CsiroDataSrc()


# ---------------------------------------------------------------------------
# CODataRegistry
# ---------------------------------------------------------------------------


def _make_registry(aodn_catalog=None, csiro_catalog=None):
    """Build a CODataRegistry with both sources fully mocked."""
    if aodn_catalog is None:
        aodn_catalog = {"aodn_dataset.parquet": {"uuid": "aaa"}}
    if csiro_catalog is None:
        csiro_catalog = {CsiroDataSrc.THE_ONLY_DATASET_NAME: {"uuid": "bbb"}}

    mock_aodn_metadata = MagicMock()
    mock_aodn_metadata.catalog = dict(aodn_catalog)

    mock_aodn_src = MagicMock()
    mock_aodn_src.get_name.return_value = AODN
    mock_aodn_src.get_metadata.return_value = mock_aodn_metadata
    mock_aodn_src.get_metadata_catalog.return_value = dict(aodn_catalog)

    mock_csiro_src = MagicMock()
    mock_csiro_src.get_name.return_value = CSIRO
    mock_csiro_src.get_metadata_catalog.return_value = dict(csiro_catalog)

    with patch(
        "data_access_service.models.co_data_source.co_data_registory.AodnDataSrc",
        return_value=mock_aodn_src,
    ), patch(
        "data_access_service.models.co_data_source.co_data_registory.CsiroDataSrc",
        return_value=mock_csiro_src,
    ):
        registry = CODataRegistry()

    return registry, mock_aodn_src, mock_csiro_src


class TestCODataRegistry:
    def test_get_metadata_merges_catalogs_from_all_sources(self):
        registry, mock_aodn_src, mock_csiro_src = _make_registry()
        metadata = registry.get_metadata()
        # Both datasets should appear in the merged catalog
        assert "aodn_dataset.parquet" in metadata.catalog
        assert CsiroDataSrc.THE_ONLY_DATASET_NAME in metadata.catalog

    def test_get_metadata_raises_on_conflicting_dataset_names(self):
        # Same key in both AODN and CSIRO catalogs → conflict
        conflict_name = "conflict.parquet"
        registry, _, _ = _make_registry(
            aodn_catalog={conflict_name: {}},
            csiro_catalog={conflict_name: {}},
        )
        with pytest.raises(Exception, match="Conflicted dataset names"):
            registry.get_metadata()

    def test_get_dataset_returns_from_first_matching_source(self):
        registry, mock_aodn_src, _ = _make_registry()
        expected = MagicMock()
        mock_aodn_src.get_dataset.return_value = expected

        result = registry.get_dataset("aodn_dataset.parquet")
        assert result is expected

    def test_get_dataset_falls_through_to_next_source_on_not_found(self):
        registry, mock_aodn_src, mock_csiro_src = _make_registry()

        # AODN raises DatasetNotFoundError, CSIRO returns the dataset
        mock_aodn_src.get_dataset.side_effect = DatasetNotFoundError(
            dataset_name=CsiroDataSrc.THE_ONLY_DATASET_NAME,
            data_source_name=AODN,
        )
        expected = MagicMock()
        mock_csiro_src.get_dataset.return_value = expected

        result = registry.get_dataset(CsiroDataSrc.THE_ONLY_DATASET_NAME)
        assert result is expected

    def test_get_dataset_raises_when_not_found_in_any_source(self):
        registry, mock_aodn_src, mock_csiro_src = _make_registry()

        mock_aodn_src.get_dataset.side_effect = DatasetNotFoundError(
            dataset_name=UNKNOWN_DATASET, data_source_name=AODN
        )
        mock_csiro_src.get_dataset.side_effect = DatasetNotFoundError(
            dataset_name=UNKNOWN_DATASET, data_source_name=CSIRO
        )

        with pytest.raises(Exception, match="not found in any data source"):
            registry.get_dataset(UNKNOWN_DATASET)
