# test_generate_csv_file.py
import unittest
from unittest.mock import patch, MagicMock
from data_access_service.tasks.generate_csv_file import process_csv_data_file


class TestGenerateCSVFile(unittest.TestCase):

    @patch("data_access_service.tasks.generate_csv_file.AWSClient")
    @patch("data_access_service.tasks.generate_csv_file.generate_started_email_subject")
    @patch("data_access_service.tasks.generate_csv_file.generate_started_email_content")
    @patch(
        "data_access_service.tasks.generate_csv_file.generate_completed_email_subject"
    )
    @patch(
        "data_access_service.tasks.generate_csv_file.generate_completed_email_content"
    )
    @patch("data_access_service.tasks.generate_csv_file._generate_csv_file")
    @patch("data_access_service.tasks.generate_csv_file._query_data")
    def test_process_csv_data_file(
        self,
        mock_query_data,
        mock_generate_csv_file,
        mock_generate_completed_email_content,
        mock_generate_completed_email_subject,
        mock_generate_started_email_content,
        mock_generate_started_email_subject,
        MockAWSClient,
    ):
        # Mock the AWSClient instance
        mock_aws_client = MockAWSClient.return_value
        mock_aws_client.upload_data_file_to_s3.return_value = (
            "http://example.com/download"
        )

        # Mock the email generation functions
        mock_generate_started_email_subject.return_value = (
            "start processing data file whose uuid is: 12345"
        )
        mock_generate_started_email_content.return_value = "Email content for start"
        mock_generate_completed_email_subject.return_value = (
            "finish processing data file whose uuid is: 12345"
        )
        mock_generate_completed_email_content.return_value = (
            "Email content for completion"
        )

        # Mock the _generate_csv_file function
        mock_generate_csv_file.return_value = "test.csv"

        # Mock the _query_data function
        mock_query_data.return_value = MagicMock()

        # Call the function to test
        process_csv_data_file(
            "12345",
            "2023-01-01",
            "2023-01-31",
            "-90",
            "90",
            "-180",
            "180",
            "example@aodn.org.au",
        )

        # Assertions
        mock_aws_client.send_email.assert_called()
        mock_aws_client.upload_data_file_to_s3.assert_called_with(
            "test.csv", "12345/test.csv"
        )
        mock_generate_csv_file.assert_called()


if __name__ == "__main__":
    unittest.main()
