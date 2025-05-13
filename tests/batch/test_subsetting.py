from unittest.mock import patch, MagicMock
from data_access_service.batch.subsetting import init

def test_init_function():
    # Mock parameters
    job_id = "test-job-id"
    parameters = {
        "uuid": "test-uuid",
        "start_date": "02-2020",
        "end_date": "04-2021",
        "multi_polygon": "test-polygon",
        "recipient": "test-recipient",
    }

    # Mock AWSClient
    with patch("data_access_service.batch.subsetting.AWSClient") as MockAWSClient:
        mock_client = MockAWSClient.return_value
        mock_client.submit_a_job = MagicMock()

        # Call the init function
        init(job_id, parameters)

        # Assert that submit_a_job was called twice
        assert mock_client.submit_a_job.call_count == 1

        # Verify the first call (data preparation job)
        mock_client.submit_a_job.assert_any_call(
            job_name="prepare-data-for-job-test-job-id",
            job_queue="generate-csv-data-file",
            job_definition="generate-csv-data-file-dev",
            parameters=parameters,
            array_size=1,  # Adjust based on the mocked date range
            dependency_job_id=job_id,
        )

        # Verify the second call (data collection job)
        # mock_client.submit_a_job.assert_any_call(
        #     job_name="collect-data-for-job-test-job-id",
        #     job_queue="generate-csv-data-file",
        #     job_definition="generate-csv-data-file-dev",
        #     parameters=parameters,
        # )