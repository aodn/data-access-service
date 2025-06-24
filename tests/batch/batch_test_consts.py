# constants for test usage
from data_access_service.batch.batch_enums import Parameters

AWS_TEST_REGION = "us-east-1"

INIT_JOB_ID = "999"

INIT_PARAMETERS = {
    Parameters.UUID.value: "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
    Parameters.START_DATE.value: "02-2010",
    Parameters.END_DATE.value: "04-2011",
    Parameters.MULTI_POLYGON.value: '{"type":"MultiPolygon","coordinates":[[[[-180,90],[-180,-90],[180,-90],[180,90],[-180,90]]]]}',
    Parameters.RECIPIENT.value: "test-recipient",
}

PREPARATION_PARAMETERS = {
    **INIT_PARAMETERS,
    Parameters.MASTER_JOB_ID.value: INIT_JOB_ID,
    Parameters.TYPE.value: "sub-setting-data-preparation",
    Parameters.DATE_RANGES.value: '{"0": ["2010-02-01", "2010-04-30"], "1": ["2010-05-01", "2010-07-31"], "2": ["2010-08-01", "2010-10-31"], "3": ["2010-11-01", "2011-01-31"], "4": ["2011-02-01", "2011-04-30"]}',
    Parameters.INTERMEDIATE_OUTPUT_FOLDER.value: "/tmp/tmp999",
}

PREPARATION_JOB_SUBMISSION_ARGS = {
    "job_name": "prepare-data-for-job-999",
    "job_queue": "generate-csv-data-file-test",
    "job_definition": "generate-csv-data-file-test",
    "parameters": PREPARATION_PARAMETERS,
    "array_size": 5,
    "dependency_job_id": INIT_JOB_ID,
}

COLLECTION_PARAMETERS = {
    **PREPARATION_PARAMETERS,
    Parameters.TYPE.value: "sub-setting-data-collection",
}

COLLECTION_JOB_SUBMISSION_ARGS = {
    "job_name": "collect-data-for-job-999",
    "job_queue": "generate-csv-data-file-test",
    "job_definition": "generate-csv-data-file-test",
    "parameters": COLLECTION_PARAMETERS,
    "dependency_job_id": "test-job-id-returned",
}
