import os
import zipfile
import io
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

    def zip_directory_to_s3(self, directory_path: str, s3_bucket: str, s3_key: str) -> str:
        """
        Zip a directory and upload it as a stream to S3 without saving to disk.

        Args:
            directory_path: Local path to the directory to zip.
            s3_bucket: S3 bucket name.
            s3_key: S3 object key (e.g., 'zips/data.zip').

        Raises:
            FileNotFoundError: If directory_path does not exist.
            botocore.exceptions.ClientError: If S3 upload fails.
            :param s3_key:
            :param s3_bucket:
            :param directory_path:
            :param s3_client:
        """
        # Validate directory
        if not os.path.isdir(directory_path):
            raise FileNotFoundError(f"Directory not found: {directory_path}")

        # Create an in-memory buffer
        buffer = io.BytesIO()

        # Create a ZIP file in memory
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Walk the directory
            for root, _, files in os.walk(directory_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Calculate relative path for ZIP archive
                    relative_path = os.path.relpath(file_path, directory_path)
                    # Add file to ZIP
                    zip_file.write(file_path, relative_path)

        # Reset buffer position to the beginning
        buffer.seek(0)

        # Upload to S3
        self.s3.upload_fileobj(buffer, s3_bucket, s3_key)
        return "s3://" + s3_bucket + "/" + s3_key

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
