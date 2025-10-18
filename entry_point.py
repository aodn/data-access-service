import os

import boto3

from data_access_service import init_log, Config, API
from data_access_service.batch import subsetting

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

# A global app
api = API()
api.initialize_metadata()

match call_type:
    case "sub-setting":
        subsetting.init(api, job_id_of_init=job_id, parameters=parameters)
    case "sub-setting-data-preparation":
        """
        Please take noted that the parameters in each call are different, the batch will call the
        first job init, and init job will add some parameter before calling the prepare_data job
        """
        subsetting.prepare_data(api, job_index=job_index, parameters=parameters)
    case "sub-setting-data-collection":
        subsetting.collect_data(parameters=parameters)
    case _:
        logger.error("Unknow call type", call_type)
