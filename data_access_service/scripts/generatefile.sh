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
print('Job ID:', job_id)

# Retrieve the job details
response = client.describe_jobs(jobs=[job_id])
job = response['jobs'][0]
print('Job:', job)

# Extract parameters from the job details
parameters = job['parameters']
print('Parameters:', parameters)

# get params
uuid = parameters['uuid']
start_date = parameters['start_date']
end_date = parameters['end_date']
multi_polygon = parameters['multi_polygon']
recipient = parameters['recipient']

print('UUID:', uuid)
print('Start Date:', start_date)
print('End Date:', end_date)
print('Multi Polygon:', multi_polygon)
print('Recipient:', recipient)


process_csv_data_file(job_id, uuid, start_date, end_date, multi_polygon, recipient)
"
