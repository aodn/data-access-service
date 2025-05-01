import os
import boto3
import logging
from data_access_service import init_log
from data_access_service.batch import subsetting

logger = init_log(logging.DEBUG)

# Initialize a boto3 client for AWS Batch
client = boto3.client('batch')

# Get the job ID from the environment variable
job_id = os.getenv('AWS_BATCH_JOB_ID')
logger.info('Job ID:', job_id)

# Retrieve the job details
response = client.describe_jobs(jobs=[job_id])
job = response['jobs'][0]
logger.info('Job:', job)

# Extract parameters from the job details
parameters = job['parameters']
logger.info('Parameters:', parameters)

# Switch based on parameter call_type
call_type = parameters['type']

match call_type:
    case "sub-setting":
        subsetting.execute(job_id, parameters)
    case _:
        logging.error('Unknow call type', call_type)