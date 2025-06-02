# constants for test usage
AWS_TEST_REGION = "us-east-1"

INIT_JOB_ID = "init-job-id"

INIT_PARAMETERS = {
    "uuid": "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
    "start_date": "02-2010",
    "end_date": "04-2011",
    "multi_polygon": '{"type":"MultiPolygon","coordinates":[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}',
    "recipient": "test-recipient",
}

PREPARATION_PARAMETERS = {
    **INIT_PARAMETERS,
    "type": "sub-setting-data-preparation",
    'date_ranges': '{"0": ["2010-02-01", "2010-04-30"], "1": ["2010-05-01", "2010-07-31"], "2": ["2010-08-01", "2010-10-31"], "3": ["2010-11-01", "2011-01-31"], "4": ["2011-02-01", "2011-04-30"]}'
}

PREPARATION_JOB_SUBMISSION_ARGS = {
    "job_name": "prepare-data-for-job-init-job-id",
    "job_queue": "generate-csv-data-file",
    "job_definition": "generate-csv-data-file-dev",
    "parameters": PREPARATION_PARAMETERS,
    "array_size": 5,
    "dependency_job_id": INIT_JOB_ID,
}

COLLECTION_PARAMETERS = {
    **PREPARATION_PARAMETERS,
    "type": "sub-setting-data-collection",
}

COLLECTION_JOB_SUBMISSION_ARGS = {
    "job_name": "collect-data-for-job-init-job-id",
    "job_queue": "generate-csv-data-file",
    "job_definition": "generate-csv-data-file-dev",
    "parameters": COLLECTION_PARAMETERS,
    "dependency_job_id": "test-job-id-returned",
}