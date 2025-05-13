import io
import os
import zipfile

import boto3

from data_access_service import init_log
from data_access_service.config.config import Config


class AWSClient:
    def __init__(self):
        self.config: Config = Config.get_config()
        self.log = init_log(self.config)
        self.log.info("Init AWS class")
        self.s3 = self.config.get_s3_client()
        self.ses = boto3.client("ses")
        self.batch = boto3.client("batch")

    def zip_directory_to_s3(
            self, directory_path: str, s3_bucket: str, s3_key: str
    ) -> str:
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
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
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
        region = self.s3.meta.region_name
        object_download_url = f"https://{s3_bucket}.s3.{region}.amazonaws.com/{s3_key}"
        return object_download_url

    def upload_to_s3(self, file_path: str, s3_bucket: str, s3_key: str) -> str:

        # Validate file
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Upload to S3
        self.s3.upload_file(file_path, s3_bucket, s3_key)
        region = self.s3.meta.region_name
        object_download_url = f"https://{s3_bucket}.s3.{region}.amazonaws.com/{s3_key}"
        return object_download_url

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

    def submit_a_job(self, job_name: str, job_queue: str, job_definition: str, parameters: dict, array_size: int = 0, dependency_job_id: str = None):
        """
        Submit a job to AWS Batch.

        Args:
            job_name: Name of the job.
            job_queue: Job queue to submit the job to.
            job_definition: Job definition to use.
            parameters: Parameters for the job.
            array_size: Size of the array job (default is 0, which means no array job).
            dependency_job_id: Job ID of the job this job depends on (default is None).

        Returns:
            The response from the AWS Batch service.
        """
        request = {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "parameters": parameters,
        }

        if array_size > 0:
            request["arrayProperties"] = {"size": array_size}

        if dependency_job_id:
            request["dependsOn"] = [{"jobId": dependency_job_id}]
        response = self.batch.submit_job(**request)

        self.log.info(f"Job submitted: {response['jobId']}")
        return response
