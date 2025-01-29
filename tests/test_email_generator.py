# test_email_generator.py
import unittest
from data_access_service.tasks.email_generator import (
    generate_started_email_subject,
    generate_completed_email_subject,
    generate_started_email_content,
    generate_completed_email_content,
)


class TestEmailGenerator(unittest.TestCase):

    def test_generate_started_email_subject(self):
        uuid = "12345"
        expected_subject = "start processing data file whose uuid is: 12345"
        self.assertEqual(generate_started_email_subject(uuid), expected_subject)

    def test_generate_completed_email_subject(self):
        uuid = "12345"
        expected_subject = "finish processing data file whose uuid is: 12345"
        self.assertEqual(generate_completed_email_subject(uuid), expected_subject)

    def test_generate_started_email_content(self):
        uuid = "12345"
        conditions = [("start date", "2023-01-01"), ("end date", "2023-01-31")]
        expected_content = (
            "already start processing12345. \nThe condition(s): "
            "start date: 2023-01-01,\n end date: 2023-01-31,\n "
            ". \nPlease wait for the result. After the process is done, you will receive another email."
        )
        self.assertEqual(
            generate_started_email_content(uuid, conditions), expected_content
        )

    def test_generate_completed_email_content(self):
        uuid = "12345"
        conditions = [("start date", "2023-01-01"), ("end date", "2023-01-31")]
        object_url = "http://example.com/download"
        expected_content = (
            "The result is ready. \nThe id of the dataset is: 12345"
            "The condition(s): start date: 2023-01-01, end date: 2023-01-31, "
            ". \nYou can download it. The download link is: http://example.com/download"
        )
        self.assertEqual(
            generate_completed_email_content(uuid, conditions, object_url),
            expected_content,
        )


if __name__ == "__main__":
    unittest.main()
