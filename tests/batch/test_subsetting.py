from data_access_service.batch.subsetting import init
from tests.batch.batch_test_consts import init_job_id, init_parameters, preparation_job_submission_args, \
    collection_job_submission_args
from unittest.mock import patch, MagicMock, call

from tests.batch.batch_test_consts import init_job_id, init_parameters, preparation_job_submission_args, \
    collection_job_submission_args


def test_init():
    # Mock AWSClient
    with patch("data_access_service.batch.subsetting.AWSClient") as MockAWSClient:
        mock_client = MockAWSClient.return_value
        mock_client.submit_a_job = MagicMock()
        mock_client.submit_a_job.return_value = "test-job-id-returned"

        # Call the init function
        init(init_job_id, init_parameters)

        expected_call_1 = call(
            job_name=preparation_job_submission_args["job_name"],
            job_queue=preparation_job_submission_args["job_queue"],
            job_definition=preparation_job_submission_args["job_definition"],
            parameters=preparation_job_submission_args["parameters"],
            array_size=3,
            dependency_job_id=init_job_id,
        )

        expected_call_2 = call(
            job_name=collection_job_submission_args["job_name"],
            job_queue=collection_job_submission_args["job_queue"],
            job_definition=collection_job_submission_args["job_definition"],
            parameters=collection_job_submission_args["parameters"],
            dependency_job_id=collection_job_submission_args["dependency_job_id"],
        )

        # Assert that submit_a_job was called twice
        assert mock_client.submit_a_job.call_count == 2
        # Assert that the expected calls were made
        assert expected_call_1 in mock_client.submit_a_job.call_args_list
        assert expected_call_2 in mock_client.submit_a_job.call_args_list


@patch("data_access_service.core.AWSClient.upload_to_s3")
def test_prepare_data(mock_upload_to_s3):
    # Create a temporary directory to simulate the local folder
    with tempfile.TemporaryDirectory() as temp_dir:
        # Mock AWSClient
        mock_aws_client = MagicMock(spec=AWSClient)
        mock_upload_to_s3.return_value = None

        # Prepare parameters
        job_id = "test-job-id"
        parameters = {
            "uuid": "test-uuid",
            "date_ranges": '{"0": ["2023-01-01", "2023-01-31"]}',
            "multi_polygon": "test-polygon",
            "recipient": "test-recipient",
        }

        # Call prepare_data
        prepare_data(job_id, 0, parameters)

        # Assert that upload_to_s3 was called for each file in the temp directory
        uploaded_files = [call[0][0] for call in mock_upload_to_s3.call_args_list]
        for root, _, files in os.walk(temp_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                assert local_file_path in uploaded_files

        # Assert that the correct S3 keys were used
        expected_s3_keys = [
            f"{job_id}/temp/{os.path.relpath(file, temp_dir)}"
            for file in uploaded_files
        ]
        actual_s3_keys = [call[0][2] for call in mock_upload_to_s3.call_args_list]
        assert set(expected_s3_keys) == set(actual_s3_keys)
