#!/bin/sh

poetry install

poetry run python -c "
import os
import boto3
import json
from data_access_service.tasks.generate_csv_file import process_csv_data_file

# Initialize a boto3 client for AWS Batch
client = boto3.client('batch')

# Get the job ID from the environment variable
job_id = os.getenv('AWS_BATCH_JOB_ID')

# Retrieve the job details
response = client.describe_jobs(jobs=[job_id])
job = response['jobs'][0]

# Extract parameters from the job details
parameters = job['parameters']

# Parse the complex object from the parameters
inputs = json.loads(parameters['inputs'])

uuid = inputs['uuid']
start_date = inputs['start_date']
end_date = inputs['end_date']
multi_polygon = inputs['multi_polygon']
recipient = inputs['recipient']

process_csv_data_file2(uuid, start_date, end_date, multi_polygon, recipient)
"
