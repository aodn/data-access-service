# src/job_processor.py
import boto3


class JobProcessor:
    def __init__(self, endpoint_url, access_key, secret_key, queue_url):
        self.queue_url = queue_url
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
        self.sqs_client = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

    def process_job(self):
        # Poll SQS for a job message
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=10
        )
        messages = response.get("Messages", [])
        if not messages:
            print("No jobs in queue")
            return

        message = messages[0]
        job_details = message["Body"].split(",")
        input_bucket, input_key, output_bucket, output_key = job_details

        # Read input from S3
        input_obj = self.s3_client.get_object(Bucket=input_bucket, Key=input_key)
        input_content = input_obj["Body"].read().decode("utf-8")

        # Process (convert to uppercase)
        output_content = input_content.upper()

        # Write output to S3
        self.s3_client.put_object(
            Bucket=output_bucket, Key=output_key, Body=output_content.encode("utf-8")
        )

        # Delete message from SQS (job completed)
        self.sqs_client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"]
        )
