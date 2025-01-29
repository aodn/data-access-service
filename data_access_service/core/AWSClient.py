import boto3
import logging
from data_access_service.config.config import load_config

log = logging.getLogger(__name__)


class AWSClient:
    def __init__(self):
        log.info("Init AWS class")
        self.s3 = boto3.client("s3")
        self.ses = boto3.client("ses")
        self.config = load_config()

    def upload_data_file_to_s3(self, file_path, s3_path):
        bucket_name = self.config["aws"]["s3"]["bucket_name"]["csv"]
        region = self.s3.meta.region_name


        try:
            self.s3.upload_file(file_path, bucket_name, s3_path)
            log.info(f"File uploaded to s3://{bucket_name}/{s3_path}")
            object_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_path}"
            return object_url
        except Exception as e:
            log.info(f"Error uploading file to s3://{bucket_name}/{s3_path}: {e}")
            raise e

    def send_email(self, recipient, subject, body_text):
        sender = self.config["aws"]["ses"]["sender_email"]

        try:
            response = self.ses.send_email(
                Source=sender,
                Destination={
                    'ToAddresses': [recipient]
                },
                Message={
                    'Subject': {
                        'Data': subject
                    },
                    'Body': {
                        'Text': {
                            'Data': body_text
                        }
                    }
                }
            )
            log.info(f"Email sent to {recipient} with message ID: {response['MessageId']}")
        except Exception as e:
            log.info(f"Error sending email to {recipient}: {e}")
            raise e