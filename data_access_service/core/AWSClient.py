import os

import boto3

from data_access_service import init_log
from data_access_service.config.config import Config, EnvType


class AWSClient:
    def __init__(self):
        self.config: Config = Config.get_config()
        self.log = init_log(self.config)
        self.log.info("Init AWS class")
        self.s3 = self.config.get_s3_client()
        self.ses = boto3.client("ses")

    def upload_data_file_to_s3(self, file_path, s3_path):
        bucket_name = self.config.get_csv_bucket_name()
        region = self.s3.meta.region_name

        try:
            self.s3.upload_file(file_path, bucket_name, s3_path)
            self.log.info(f"File uploaded to s3://{bucket_name}/{s3_path}")
            object_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_path}"
            return object_url
        except Exception as e:
            self.log.info(f"Error uploading file to s3://{bucket_name}/{s3_path}: {e}")
            raise e

    def send_email(self, recipient, subject, body_text):
        sender = self.config.get_sender_email()

        try:
            response = self.ses.send_email(
                Source=sender,
                Destination={"ToAddresses": [recipient]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": body_text}},
                },
            )
            self.log.info(
                f"Email sent to {recipient} with message ID: {response['MessageId']}"
            )
        except Exception as e:
            self.log.info(f"Error sending email to {recipient}: {e}")
            raise e
