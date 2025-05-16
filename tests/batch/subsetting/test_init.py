from unittest.mock import patch, MagicMock, call

from data_access_service.batch.subsetting import init, prepare_data
from tests.batch.batch_test_consts import PREPARATION_PARAMETERS
from tests.batch.batch_test_consts import INIT_JOB_ID, INIT_PARAMETERS, PREPARATION_JOB_SUBMISSION_ARGS, \
    COLLECTION_JOB_SUBMISSION_ARGS


def test_init():
    # Mock AWSClient
    with patch("data_access_service.batch.subsetting.AWSClient") as MockAWSClient:
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


@patch("boto3.s3.transfer.S3Transfer.upload_file")
def test_preparing_data(mock_upload_file):
    # Mock the upload_file method to do nothing
    mock_upload_file.return_value = None

    prepare_data(INIT_JOB_ID, 0, PREPARATION_PARAMETERS)
    # Assert that the upload_file method was called with the expected arguments
    mock_upload_file.assert_called_once_with(
        "test-uuid-02-2020.csv",
        "test-bucket",
        "test-uuid/test-uuid-02-2020.csv"
    )