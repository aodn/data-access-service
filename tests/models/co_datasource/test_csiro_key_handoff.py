"""CSIRO keys fetched by a Batch init job can be handed to its children."""

import json
import logging
import time
from unittest.mock import patch

import pytest

from data_access_service.config.config import Config
from data_access_service.models.co_datasource.co_data_registory import (
    resolve_dataset_location,
)
from data_access_service.models.co_datasource.csiro import csiro_data_src
from data_access_service.models.co_datasource.csiro.csiro_data_src import (
    accept_csiro_keys,
    export_csiro_keys,
    redact_job_parameters,
)

from tests.models.co_datasource.test_dataset_location import (
    CSIRO_DATASET,
    _patch_keys,
)


def _handoff_entry(**overrides) -> dict:
    return {
        "dataset_name": CSIRO_DATASET,
        "fedora_pid": "csiro:72626",
        "bucket": "dapprd-mnf",
        "prefix": "000072626v004/data/",
        "endpoint_url": "https://s3.data.csiro.au",
        "access_key": "csiro-key",
        "secret_access_key": "csiro-secret",
        "fetched_at": time.time(),
        **overrides,
    }


def test_parent_key_is_exported_and_used_without_a_child_request():
    with _patch_keys() as parent_request:
        parent_location = resolve_dataset_location(CSIRO_DATASET)

    raw = export_csiro_keys()
    assert raw is not None
    assert parent_request.call_count == 2

    csiro_data_src._fetched_here.clear()
    csiro_data_src._handed_down.clear()
    accept_csiro_keys(raw)

    with patch(
        "data_access_service.models.co_datasource.csiro.csiro_data_src.requests.get"
    ) as child_request:
        child_location = resolve_dataset_location(CSIRO_DATASET)

    child_request.assert_not_called()
    assert child_location == parent_location


def test_expired_parent_key_makes_the_child_fetch():
    ttl_seconds = Config.get_config().get_csiro_config().key_cache_ttl_seconds
    raw = json.dumps([_handoff_entry(fetched_at=time.time() - ttl_seconds - 1)])

    accept_csiro_keys(raw)

    with _patch_keys() as child_request:
        location = resolve_dataset_location(CSIRO_DATASET)

    assert child_request.call_count == 2
    assert location.access_key == "csiro-key"


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "not-json-secret-value",
        json.dumps([_handoff_entry(fedora_pid="csiro:different")]),
    ],
)
def test_unusable_parent_value_is_ignored_and_the_child_fetches(raw, caplog):
    with caplog.at_level(logging.WARNING):
        accept_csiro_keys(raw)

    with _patch_keys() as child_request:
        resolve_dataset_location(CSIRO_DATASET)

    assert child_request.call_count == 2
    assert "not-json-secret-value" not in caplog.text


def test_export_returns_none_when_this_process_fetched_nothing():
    assert export_csiro_keys() is None


def test_redact_job_parameters_hides_the_key_without_changing_the_input():
    parameters = {
        "type": "sub-setting-data-preparation",
        "csiro_keys": "secret-json",
    }

    redacted = redact_job_parameters(parameters)

    assert redacted == {
        "type": "sub-setting-data-preparation",
        "csiro_keys": "<redacted>",
    }
    assert parameters["csiro_keys"] == "secret-json"
