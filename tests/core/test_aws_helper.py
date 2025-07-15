import requests

from botocore.exceptions import ClientError
from data_access_service.config.config import IntTestConfig, Config
from data_access_service.core.AWSHelper import AWSHelper
from tests.core.test_with_s3 import TestWithS3


class TestAWSHelper(TestWithS3):

    def test_email_client_format(
        self, setup, aws_clients, localstack, mock_boto3_client
    ):
        _, _, ses_client = aws_clients
        config = Config.get_config()
        config.set_ses_client(ses_client)

        helper = AWSHelper()
        receipt = "receipt@test.com"
        subject = "This is a test"
        links = ["http://test/test.zip", "https://test/test1.zip"]

        # Need to setup email verify otherwise SES will reject sent
        ses_client.verify_email_identity(EmailAddress=config.get_sender_email())
        helper.send_email(receipt, subject, links)

        # Retrieve sent emails from LocalStack SES endpoint
        try:
            response = (
                ses_client.get_paginator("list_identities")
                .paginate()
                .build_full_result()
            )
            assert response["Identities"] == [
                config.get_sender_email()
            ], "Verified identities"
        except ClientError as e:
            print(f"Error listing identities: {e.response['Error']['Message']}")

        # Fetch email content from LocalStack SES endpoint
        try:
            response = requests.get(localstack.get_url() + "/_aws/ses")
            response.raise_for_status()  # Raise exception for HTTP errors
            emails = response.json().get("messages", [])

            if not emails:
                assert False, "No emails found in LocalStack SES."

            # Find the latest email (or filter by Message ID if needed)
            for email in emails:
                if (
                    email["Source"] == config.get_sender_email()
                    and email["Subject"] == subject
                ):
                    assert (
                        email["Source"] == config.get_sender_email()
                    ), "Source correct"
                    assert email["Destination"]["ToAddresses"] == [
                        receipt
                    ], "ToAddress correct"
                    assert email["Subject"] == subject, "Subject correct"
                    assert (
                        email["Body"]["text_part"]
                        == "Hello, User!\nPlease use the link below to download the files"
                    ), "Text correct"
                    assert (
                        email["Body"]["html_part"].strip()
                        == """<html>\n        <body>\n            [\'<a href="http://test/test.zip">http://test/test.zip</a>\', \'<a href="https://test/test1.zip">https://test/test1.zip</a>\']\n        </body>\n        </html>"""
                    ), "Html correct"
                else:
                    assert False, "Email not found in LocalStack SES."

        except requests.exceptions.RequestException as e:
            assert False, f"Error fetching emails: {e}"
