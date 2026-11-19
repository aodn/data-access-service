"""Processing failures notify the recipient and still fail the Batch job."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from data_access_service.batch.subsetting import subsetting_main
from data_access_service.core.data_index_files import ORIGINAL_NETCDF_KEY
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_email import (
    get_start_download_email_html_body,
)
from data_access_service.utils.email_templates.error_email import (
    ERROR_EMAIL_SUBJECT,
    get_error_email_html_body,
)
from data_access_service.utils.email_templates.no_data_email import (
    NO_DATA_EMAIL_SUBJECT,
    get_no_data_email_html_body,
)

# init emails "started" before any of these stages run. collect_data does not.
INIT_STAGES = {"normalize", "resolve", "zarr", "original", "submit"}


@pytest.fixture
def workflow(monkeypatch):
    request = SubsetRequest(
        uuid="test-uuid",
        keys=["data.zarr"],
        start_date="2020-01-01",
        end_date="2020-01-31",
        recipient="user@example.com",
        output_format="netcdf",
    )
    config = MagicMock()
    monkeypatch.setattr(subsetting_main.Config, "get_config", lambda: config)
    logger = MagicMock()
    monkeypatch.setattr(subsetting_main, "init_log", lambda config: logger)
    aws = MagicMock()
    monkeypatch.setattr(subsetting_main, "AWSHelper", lambda: aws)
    monkeypatch.setattr(
        subsetting_main, "get_subset_request", lambda parameters: request
    )
    monkeypatch.setattr(
        subsetting_main, "normalize_request", lambda api, request: request
    )
    resolved = SimpleNamespace(
        has_data=True,
        uuid=request.uuid,
        keys=request.keys,
        start_date=request.start_date,
        end_date=request.end_date,
    )
    monkeypatch.setattr(
        subsetting_main, "resolve_subset_request", lambda **kwargs: resolved
    )
    parameters = {
        "master_job_id": "parent-job",
        "intermediate_output_folder": "unused",
        "date_ranges": json.dumps({"0": ["2020-01-01", "2020-01-31"]}),
    }
    return SimpleNamespace(
        request=request,
        aws=aws,
        logger=logger,
        resolved=resolved,
        parameters=parameters,
    )


def _start_email(request: SubsetRequest) -> dict:
    return {
        "recipient": request.recipient,
        "subject": f"Start processing data file whose uuid is: {request.uuid}",
        "html_body": get_start_download_email_html_body(request),
    }


def _error_email(request: SubsetRequest) -> dict:
    return {
        "recipient": request.recipient,
        "subject": ERROR_EMAIL_SUBJECT.format(uuid=request.uuid),
        "html_body": get_error_email_html_body(request),
    }


def run_stage(monkeypatch, workflow, stage, process):
    """Replace processing with a mock at each independently executed boundary."""
    if stage in ("normalize", "resolve", "zarr", "original", "submit"):
        target = {
            "normalize": "normalize_request",
            "resolve": "resolve_subset_request",
            "zarr": "run_zarr_subset",
            "original": "run_data_index_download",
        }
        if stage == "original":
            # The data-index key sends init to the data-index download.
            workflow.request = replace(workflow.request, keys=[ORIGINAL_NETCDF_KEY])
            monkeypatch.setattr(
                subsetting_main,
                "get_subset_request",
                lambda parameters: workflow.request,
            )
        if stage == "submit":
            workflow.resolved.keys = ["data.parquet"]
            monkeypatch.setattr(
                subsetting_main, "check_key_supports_format", MagicMock()
            )
            monkeypatch.setattr(
                subsetting_main, "uses_polygon_sharding", lambda *args: False
            )
            monkeypatch.setattr(
                subsetting_main, "split_date_range", lambda **kwargs: [("start", "end")]
            )
            monkeypatch.setattr(subsetting_main, "export_csiro_keys", lambda: None)
            workflow.aws.submit_a_job = process
        else:
            monkeypatch.setattr(subsetting_main, target[stage], process)
        return subsetting_main.init(MagicMock(), "parent-job", workflow.parameters)
    if stage in ("prepare", "prepare_polygon"):
        if stage == "prepare_polygon":
            workflow.parameters["polygon_ranges"] = "{}"
            monkeypatch.setattr(subsetting_main, "_prepare_polygon_range_data", process)
        else:
            monkeypatch.setattr(subsetting_main, "process_parquet_files", process)
        return subsetting_main.prepare_data(MagicMock(), None, workflow.parameters)
    monkeypatch.setattr(subsetting_main, "collect_parquet_files", process)
    return subsetting_main.collect_data(workflow.parameters)


@pytest.mark.parametrize(
    "stage",
    [
        "normalize",
        "resolve",
        "zarr",
        "original",
        "submit",
        "collect",
    ],
)
@pytest.mark.parametrize("notification_fails", [False, True])
def test_processing_failure_notifies_and_preserves_original_error(
    monkeypatch,
    workflow,
    stage,
    notification_fails,
):
    error = RuntimeError("Processing failed")
    if notification_fails:
        # The start email must still succeed. Failing every send would raise
        # before processing and the original error would never be the one re-raised.
        def fail_error_email(**kwargs):
            if kwargs.get("subject", "").startswith("Error processing"):
                raise RuntimeError("SES unavailable")

        workflow.aws.send_email.side_effect = fail_error_email
    with pytest.raises(RuntimeError) as caught:
        run_stage(monkeypatch, workflow, stage, MagicMock(side_effect=error))
    assert caught.value is error
    expected = []
    if stage in INIT_STAGES:
        expected.append(call(**_start_email(workflow.request)))
    expected.append(call(**_error_email(workflow.request)))
    assert workflow.aws.send_email.call_args_list == expected
    if notification_fails:
        workflow.logger.exception.assert_called_once_with(
            "Failed to send download error email"
        )


@pytest.mark.parametrize("stage", ["prepare", "prepare_polygon"])
def test_preparation_failure_records_marker_instead_of_emailing(
    monkeypatch, workflow, stage
):
    """Many children run per request, so a child must never email; it records
    the failure and exits successfully so the collection job still runs."""
    error = RuntimeError("Processing failed")
    record = MagicMock(return_value=True)
    monkeypatch.setattr(subsetting_main, "record_preparation_failure", record)

    result = run_stage(monkeypatch, workflow, stage, MagicMock(side_effect=error))

    assert result is None
    record.assert_called_once_with("parent-job", "0", error)
    workflow.aws.send_email.assert_not_called()


@pytest.mark.parametrize("stage", ["prepare", "prepare_polygon"])
def test_preparation_failure_reraises_when_marker_cannot_be_written(
    monkeypatch, workflow, stage
):
    error = RuntimeError("Processing failed")
    monkeypatch.setattr(
        subsetting_main, "record_preparation_failure", MagicMock(return_value=False)
    )
    with pytest.raises(RuntimeError) as caught:
        run_stage(monkeypatch, workflow, stage, MagicMock(side_effect=error))
    assert caught.value is error
    workflow.aws.send_email.assert_not_called()


@pytest.mark.parametrize(
    "stage", ["zarr", "original", "prepare", "prepare_polygon", "collect"]
)
def test_success_does_not_send_error_email(monkeypatch, workflow, stage):
    process = MagicMock(return_value="output-folder")
    result = run_stage(monkeypatch, workflow, stage, process)
    process.assert_called_once()
    if stage in ("zarr", "original"):
        workflow.aws.send_email.assert_called_once_with(
            **_start_email(workflow.request)
        )
    else:
        workflow.aws.send_email.assert_not_called()
    if stage in ("prepare", "prepare_polygon"):
        assert result == "output-folder"


def test_no_data_sends_start_email_then_no_data_email(monkeypatch, workflow):
    workflow.resolved.has_data = False
    process = MagicMock()
    run_stage(monkeypatch, workflow, "zarr", process)
    process.assert_not_called()
    assert workflow.aws.send_email.call_args_list == [
        call(**_start_email(workflow.request)),
        call(
            recipient=workflow.request.recipient,
            subject=NO_DATA_EMAIL_SUBJECT,
            html_body=get_no_data_email_html_body(workflow.request),
        ),
    ]


@pytest.mark.parametrize("stage", ["init", "prepare", "collect"])
def test_request_parsing_failure_does_not_attempt_notification(
    monkeypatch, workflow, stage
):
    error = ValueError("Invalid request")
    monkeypatch.setattr(
        subsetting_main, "get_subset_request", MagicMock(side_effect=error)
    )
    with pytest.raises(ValueError) as caught:
        if stage == "init":
            subsetting_main.init(MagicMock(), "parent-job", {})
        elif stage == "prepare":
            subsetting_main.prepare_data(MagicMock(), None, {})
        else:
            subsetting_main.collect_data({"master_job_id": "parent-job"})
    assert caught.value is error
    workflow.aws.send_email.assert_not_called()
