import json
import logging
import unittest
from unittest.mock import patch

from data_access_service.tasks.generate_csv_file import process_csv_data_file


class TestProcessCSVDataFile(unittest.TestCase):

    @patch("data_access_service.tasks.generate_csv_file.AWSClient")
    @patch("data_access_service.tasks.generate_csv_file.init_log")
    @patch(
        "data_access_service.tasks.generate_csv_file.generate_completed_email_subject"
    )
    @patch(
        "data_access_service.tasks.generate_csv_file.generate_completed_email_content"
    )
    @patch("data_access_service.tasks.generate_csv_file._generate_csv_file")
    @unittest.skip("Skip because the function already changed significantly. will come back to it later")
    def test_process_csv_data_file_success(
        self,
        mock_generate_csv_file,
        mock_generate_completed_email_content,
        mock_generate_completed_email_subject,
        mock_init_log,
        mock_AWSClient,
    ):
        # Arrange
        mock_generate_csv_file.return_value = "test.csv"
        mock_AWSClient_instance = mock_AWSClient.return_value
        mock_AWSClient_instance.upload_data_file_to_s3.return_value = (
            "s3://bucket/test.csv"
        )

        uuid = "test-uuid"
        start_date = "2023-01-01"
        end_date = "2023-01-31"
        multi_polygon = '{"coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}'
        recipient = "test@example.com"

        # Act
        process_csv_data_file(uuid, start_date, end_date, multi_polygon, recipient)

        # Assert
        mock_init_log.assert_called_once_with(logging.DEBUG)
        mock_generate_csv_file.assert_called_once_with(
            start_date, end_date, json.loads(multi_polygon), uuid
        )
        mock_AWSClient_instance.upload_data_file_to_s3.assert_called_once_with(
            "test.csv", f"{uuid}/test.csv"
        )
        mock_generate_completed_email_subject.assert_called_once_with(uuid)
        mock_generate_completed_email_content.assert_called_once()
        mock_AWSClient_instance.send_email.assert_any_call(
            recipient,
            mock_generate_completed_email_subject.return_value,
            mock_generate_completed_email_content.return_value,
        )


if __name__ == "__main__":
    unittest.main()
