# constants for test usage

init_job_id = "init-job-id"
init_parameters = {
    "uuid": "test-uuid",
    "start_date": "02-2020",
    "end_date": "04-2021",
    "multi_polygon": "test-polygon",
    "recipient": "test-recipient",
}

_preparation_parameters = {
    **init_parameters,
    "type": "sub-setting-data-preparation",
    "date_ranges":'{"0": ["02-2020", "03-2020", "04-2020", "05-2020", "06-2020", "07-2020", "08-2020", "09-2020", "10-2020", "11-2020", "12-2020", "01-2021", "02-2021", "03-2021", "04-2021"]}'
}

preparation_job_submission_args = {
    "job_name": "prepare-data-for-job-init-job-id",
    "job_queue": "generate-csv-data-file",
    "job_definition": "generate-csv-data-file-dev",
    "parameters": _preparation_parameters,
    "array_size": 3,
    "dependency_job_id": init_job_id,
}

_collection_parameters = {
    **init_parameters,
    "type": "sub-setting-data-collection",
    "date_ranges":'{"0": ["02-2020", "03-2020", "04-2020", "05-2020", "06-2020", "07-2020", "08-2020", "09-2020", "10-2020", "11-2020", "12-2020", "01-2021", "02-2021", "03-2021", "04-2021"]}'
}

collection_job_submission_args = {
    "job_name": "collect-data-for-job-init-job-id",
    "job_queue": "generate-csv-data-file",
    "job_definition": "generate-csv-data-file-dev",
    "parameters": _collection_parameters,
    "dependency_job_id": "test-job-id-returned",
}