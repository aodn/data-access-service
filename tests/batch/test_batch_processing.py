import pytest
import boto3
from testcontainers.localstack import LocalStackContainer
from jobprocessor import JobProcessor

@pytest.fixture(scope="module")
def localstack():
    # Start LocalStack with SQS and S3
    with LocalStackContainer(image="localstack/localstack:4.3.0") as localstack:
        yield localstack

@pytest.fixture(scope="module")
def aws_clients(localstack):
    # Initialize AWS clients pointing to LocalStack
    s3_client = boto3.client(
        "s3",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=localstack.get_url(),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )
    return s3_client, sqs_client

@pytest.fixture(scope="module")
def setup_resources(aws_clients):
    s3_client, sqs_client = aws_clients

    # Create S3 buckets
    s3_client.create_bucket(Bucket="input-bucket")
    s3_client.create_bucket(Bucket="output-bucket")

    # Create SQS queue
    response = sqs_client.create_queue(QueueName="job-queue")
    queue_url = response["QueueUrl"]

    # Upload sample input file to S3
    with open("/tmp/input.txt", "w") as f:
        f.write("hello world")
    s3_client.upload_file("/tmp/input.txt", "input-bucket", "input.txt")

    return queue_url

def test_job_processing(localstack, aws_clients, setup_resources):
    s3_client, sqs_client = aws_clients
    queue_url = setup_resources

    # Submit a job to SQS
    job_message = "input-bucket,input.txt,output-bucket,output.txt"
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=job_message
    )

    # Simulate AWS Batch job by running JobProcessor
    processor = JobProcessor(
        endpoint_url=localstack.get_url(),
        access_key="test",
        secret_key="test",
        queue_url=queue_url
    )
    processor.process_job()

    # Verify output in S3
    output_obj = s3_client.get_object(Bucket="output-bucket", Key="output.txt")
    output_content = output_obj["Body"].read().decode("utf-8")
    assert output_content == "HELLO WORLD", "Output content should be uppercase input"