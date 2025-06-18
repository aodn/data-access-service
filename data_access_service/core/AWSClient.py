import os

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

    def upload_file_to_s3(self, file_path: str, s3_bucket: str, s3_key: str) -> str:
        """
        Upload a file to an S3 bucket. Must be a file, not a file-like object.
        Args:
            file_path: Path to the file to upload.
            s3_bucket: Name of the S3 bucket.
            s3_key: Key under which to store the file in S3.
        """

        # Validate file
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Upload to S3
        self.s3.upload_file(file_path, s3_bucket, s3_key)
        region = self.s3.meta.region_name
        object_download_url = f"https://{s3_bucket}.s3.{region}.amazonaws.com/{s3_key}"
        return object_download_url

    def upload_fileobj_to_s3(self, file_obj: any, s3_bucket: str, s3_key: str) -> str:
        """
        Upload a file-like object to an S3 bucket.
        Args:
            file_obj: A file-like object to upload.
            s3_bucket: Name of the S3 bucket.
            s3_key: Key under which to store the file in S3.
        """
        try:
            self.s3.upload_fileobj(file_obj, s3_bucket, s3_key)
            region = self.s3.meta.region_name
            object_download_url = (
                f"https://{s3_bucket}.s3.{region}.amazonaws.com/{s3_key}"
            )
            return object_download_url
        except Exception as e:
            self.log.error(f"Error uploading file object to S3: {e}")
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

    def submit_a_job(
        self,
        job_name: str,
        job_queue: str,
        job_definition: str,
        parameters: dict,
        array_size: int = 1,
        dependency_job_id: str = None,
    ) -> str:
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

        if array_size > 1:
            request["arrayProperties"] = {"size": array_size}

        if dependency_job_id:
            request["dependsOn"] = [{"jobId": dependency_job_id}]
        response = self.batch.submit_job(**request)

        self.log.info(f"Job submitted: {response['jobId']}")
        # return job id
        return response["jobId"]

    def get_s3_keys(self, bucket_name: str, folder_prefix: str) -> list:
        """
        Retrieve all S3 keys in a specified folder within a bucket. (no folders, only files)
        Args:
            bucket_name: Name of the S3 bucket.
            folder_prefix: Prefix of the folder to list keys from.
        """
        keys = []
        continuation_token = None

        while True:
            list_kwargs = {
                "Bucket": bucket_name,
                "Prefix": folder_prefix,
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = self.s3.list_objects_v2(**list_kwargs)

            if "Contents" in response:
                for obj in response["Contents"]:
                    # Filter out folders (if any)
                    if obj["Key"].endswith("/"):
                        continue
                    keys.append(obj["Key"])

            if response.get("IsTruncated"):  # Check if there are more keys to fetch
                continuation_token = response["NextContinuationToken"]
            else:
                break

        return keys

    def get_s3_object(self, bucket_name: str, s3_key: str) -> bytes | None:
        """
        Retrieve an object from S3 bucket by its key.
        Args:
            bucket_name: Name of the S3 bucket.
            s3_key: Key of the object to retrieve.

        Returns:
            The content of the object as bytes, or None if the object does not exist.
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=s3_key)
            return response["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            self.log.error(f"Object {s3_key} not found in bucket {bucket_name}.")
            return None

    def get_batch_job_definition_config(self, job_definition_name: str) -> dict:
        """
        Retrieve the JSON configuration of a job definition from AWS Batch.
        Args:
            job_definition_name: Name of the job definition to retrieve.

        Returns:
            The JSON configuration of the job definition.
        """
        try:
            response = self.batch.describe_job_definitions(
                jobDefinitionName=job_definition_name,
                status="ACTIVE",
            )
            job_definitions = response.get("jobDefinitions", [])
            if not job_definitions:
                self.log.error(
                    f"No active job definitions found for: {job_definition_name}"
                )
                return {}
            # Get the latest revision of the job definition
            latest_job_definition = max(job_definitions, key=lambda x: x["revision"])
            return latest_job_definition
        except Exception as e:
            self.log.error(f"Error retrieving job definition config: {e}")
            raise e

    def get_batch_job_queue_config(self, job_queue_name: str) -> dict:
        """
        Retrieve the JSON configuration of a job queue from AWS Batch.
        Args:
            job_queue_name: Name of the job queue to retrieve.

        Returns:
            The JSON configuration of the job queue.
        """
        try:
            response = self.batch.describe_job_queues(jobQueues=[job_queue_name])
            job_queues = response.get("jobQueues", [])
            if not job_queues:
                self.log.error(f"No job queues found for: {job_queue_name}")
                return {}
            for job_queue in job_queues:
                if job_queue["jobQueueName"] == job_queue_name:
                    return job_queue
            self.log.warning(f"Job queue {job_queue_name} not found in the response.")
            return {}  # Return the first (and should be only) job queue
        except Exception as e:
            self.log.error(f"Error retrieving job queue config: {e}")
            raise e

    def get_batch_compute_environment_config(
        self, compute_environment_name: str
    ) -> dict:
        """
        Retrieve the JSON configuration of a compute environment from AWS Batch.
        Args:
            compute_environment_name: Name of the compute environment to retrieve.

        Returns:
            The JSON configuration of the compute environment.
        """
        try:
            response = self.batch.describe_compute_environments(
                computeEnvironments=[compute_environment_name]
            )
            compute_environments = response.get("computeEnvironments", [])
            if not compute_environments:
                self.log.error(
                    f"No compute environments found for: {compute_environment_name}"
                )
                return {}
            for env in compute_environments:
                if env["computeEnvironmentName"] == compute_environment_name:
                    return env
            self.log.warning(
                f"Compute environment {compute_environment_name} not found in the response."
            )
            return {}  # Return the first (and should be only) compute environment
        except Exception as e:
            self.log.error(f"Error retrieving compute environment config: {e}")
            raise e

    def does_compute_environment_need_update(
        self, compute_environment_name: str, local_compute_environment: dict
    ) -> bool:
        remote_compute_environment = self.get_batch_compute_environment_config(
            compute_environment_name
        )
        for key, value in local_compute_environment.items():
            # When updating a compute environment, we need to use "computeEnvironment" to instead "computeEnvironmentName"
            if (
                key == "computeEnvironment"
                and value != remote_compute_environment["computeEnvironmentName"]
            ):
                raise ValueError(
                    f"Compute environment name mismatch: local {value} vs remote {remote_compute_environment['computeEnvironmentName']}"
                )
            if value != remote_compute_environment.get(key):
                self.log.info(
                    f"Compute environment {compute_environment_name} needs update"
                )
                return True

        return False

    def register_batch_job_definition(self, job_definition: dict):
        response = self.batch.register_job_definition(**job_definition)
        self.log.info(
            f"Job definition registered successfully"
        )
        return response

    def update_batch_job_queue(self, job_queue: dict):
        response = self.batch.update_job_queue(**job_queue)
        self.log.info(f"Job queue updated successfully")
        return response

    def update_batch_compute_environment(self, compute_environment: dict):
        response = self.batch.update_compute_environment(**compute_environment)
        self.log.info(
            f"Compute environment updated successfully"
        )
        return response
