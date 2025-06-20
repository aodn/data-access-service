from datetime import datetime
from unittest.mock import patch, MagicMock, call

from data_access_service.batch.subsetting import init
from tests.batch.batch_test_consts import (
    INIT_JOB_ID,
    INIT_PARAMETERS,
    PREPARATION_JOB_SUBMISSION_ARGS,
    COLLECTION_JOB_SUBMISSION_ARGS,
)


@patch("data_access_service.batch.subsetting.AWSClient")
@patch("data_access_service.core.api.API.get_temporal_extent")
@patch("data_access_service.config.config.Config.get_month_count_per_job")
def test_init(mock_get_month_count_per_job, mock_get_temporal_extent, MockAWSClient):

    mock_get_month_count_per_job.return_value = 3
    # Mock the get_temporal_extent method to return a fixed value
    mock_get_temporal_extent.return_value = (
        datetime(1970, 1, 1),
        datetime(2024, 12, 31),
    )

    mock_client = MockAWSClient.return_value
    mock_client.submit_a_job = MagicMock()
    mock_client.submit_a_job.return_value = "test-job-id-returned"

    # Call the init function
    init(INIT_JOB_ID, INIT_PARAMETERS)

    expected_call_1 = call(
        job_name=PREPARATION_JOB_SUBMISSION_ARGS["job_name"],
        job_queue=PREPARATION_JOB_SUBMISSION_ARGS["job_queue"],
        job_definition=PREPARATION_JOB_SUBMISSION_ARGS["job_definition"],
        parameters=PREPARATION_JOB_SUBMISSION_ARGS["parameters"],
        array_size=PREPARATION_JOB_SUBMISSION_ARGS["array_size"],
        dependency_job_id=INIT_JOB_ID,
    )

    expected_call_2 = call(
        job_name=COLLECTION_JOB_SUBMISSION_ARGS["job_name"],
        job_queue=COLLECTION_JOB_SUBMISSION_ARGS["job_queue"],
        job_definition=COLLECTION_JOB_SUBMISSION_ARGS["job_definition"],
        parameters=COLLECTION_JOB_SUBMISSION_ARGS["parameters"],
        dependency_job_id=COLLECTION_JOB_SUBMISSION_ARGS["dependency_job_id"],
    )

    # Assert that submit_a_job was called twice
    assert mock_client.submit_a_job.call_count == 2
    # Assert that the expected calls were made
    assert expected_call_1 in mock_client.submit_a_job.call_args_list
    assert expected_call_2 in mock_client.submit_a_job.call_args_list


@patch("data_access_service.batch.subsetting.AWSClient")
@patch("data_access_service.core.api.API.get_temporal_extent")
@patch("data_access_service.config.config.Config.get_month_count_per_job")
def test_init_with_very_narrow_date_range(
    mock_get_month_count_per_job, mock_get_temporal_extent, MockAWSClient
):
    mock_get_month_count_per_job.return_value = (
        1200  # Set a very high month count to ensure no splitting occurs
    )
    # Mock the get_temporal_extent method to return a fixed value
    mock_get_temporal_extent.return_value = (
        datetime(1970, 1, 1),
        datetime(2024, 12, 31),
    )

    mock_client = MockAWSClient.return_value
    mock_client.submit_a_job = MagicMock()
    mock_client.submit_a_job.return_value = "test-job-id-returned"

    # Call the init function
    init(INIT_JOB_ID, INIT_PARAMETERS)

    expected_call_1 = call(
        job_name=PREPARATION_JOB_SUBMISSION_ARGS["job_name"],
        job_queue=PREPARATION_JOB_SUBMISSION_ARGS["job_queue"],
        job_definition=PREPARATION_JOB_SUBMISSION_ARGS["job_definition"],
        # parameters=PREPARATION_JOB_SUBMISSION_ARGS["parameters"],
        parameters={
            **PREPARATION_JOB_SUBMISSION_ARGS["parameters"],
            "date_ranges": '{"0": ["2010-02-01", "2011-04-30"]}',
        },
        array_size=1,
        dependency_job_id=INIT_JOB_ID,
    )

    expected_call_2 = call(
        job_name=COLLECTION_JOB_SUBMISSION_ARGS["job_name"],
        job_queue=COLLECTION_JOB_SUBMISSION_ARGS["job_queue"],
        job_definition=COLLECTION_JOB_SUBMISSION_ARGS["job_definition"],
        parameters={
            **COLLECTION_JOB_SUBMISSION_ARGS["parameters"],
            "date_ranges": '{"0": ["2010-02-01", "2011-04-30"]}',
        },
        dependency_job_id=COLLECTION_JOB_SUBMISSION_ARGS["dependency_job_id"],
    )

    # Assert that submit_a_job was called twice
    assert mock_client.submit_a_job.call_count == 2
    # Assert that the expected calls were made
    assert expected_call_1 in mock_client.submit_a_job.call_args_list
    assert expected_call_2 in mock_client.submit_a_job.call_args_list
