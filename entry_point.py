import os

import boto3

from data_access_service import init_log, Config
from data_access_service.batch import subsetting
from data_access_service.server import api_setup, app

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

# this one is a global api instance for batch running
batch_api = api_setup(app)

match call_type:
    case "sub-setting":
        subsetting.init(job_id_of_init=job_id, parameters=parameters)
    case "sub-setting-data-preparation":
        subsetting.prepare_data(job_index=job_index, parameters=parameters)
    case "sub-setting-data-collection":
        subsetting.collect_data(parameters=parameters)
    case _:
        logger.error("Unknow call type", call_type)
