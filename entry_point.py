import os

import boto3

from data_access_service import init_log, Config
from data_access_service.batch import subsetting
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)

logger = init_log(Config.get_config())

# Initialize a boto3 client for AWS Batch
client = boto3.client("batch")

# Get the job ID from the environment variable
job_id = os.getenv("AWS_BATCH_JOB_ID")
logger.info(f"Job ID:{job_id}")

# Get the index of the child job
job_index = os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")
if job_index is not None:
    logger.info(f"Job Index: { job_index }")

# Retrieve the job details
response = client.describe_jobs(jobs=[job_id])
job = response["jobs"][0]

# Extract parameters from the job details
parameters = job["parameters"]
logger.info(f"Parameters: {parameters}")

# Switch based on parameter call_type
call_type = parameters["type"]

# Your dummy data dictionary
dummy_data = {
    "uuid": "test-uuid-12345",
    "keys": ["var1", "var2"],
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "bboxes": [
        BoundingBox(min_lat=-45.0, max_lat=-42.0, min_lon=145.0, max_lon=148.0),
    ],
    "recipient": "yujie.bian@utas.edu.au",
    "collection_title": "Test Collection"
}

# Step 1: Create SubsetRequest from the dictionary
subset_request = SubsetRequest(**dummy_data)

# Step 2: Define your object URLs
object_urls = [
    "https://my-bucket.s3.ap-southeast-2.amazonaws.com/data/subset_001.zip",
    "https://my-bucket.s3.ap-southeast-2.amazonaws.com/data/subset_002.zip"
]

# Step 3: Generate the HTML body string
html_body = get_download_email_html_body(
    subset_request=subset_request,
    object_urls=object_urls
)

# Step 4: Send the email with the HTML string
aws=AWSHelper()
aws.send_email(
    recipient=subset_request.recipient,
    subject="Your data subset is ready",
    html_body=html_body  # ✅ This is now a string, not a dict or function
)

# match call_type:
#     case "sub-setting":
#         subsetting.init(job_id_of_init=job_id, parameters=parameters)
#     case "sub-setting-data-preparation":
#         subsetting.prepare_data(job_index=job_index, parameters=parameters)
#     case "sub-setting-data-collection":
#         subsetting.collect_data(parameters=parameters)
#     case _:
#         logger.error("Unknow call type", call_type)
