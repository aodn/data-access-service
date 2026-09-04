import os

import boto3

from data_access_service import API, Config, init_log
from data_access_service.batch import subsetting
from data_access_service.batch.estimation.generator import (
    generate_estimation_index_for_all_parquets,
)
from data_access_service.batch.pmtiles.generator import (
    generate_pmtiles_for_all_parquets,
)
from data_access_service.batch.sites_parquet.refresher import (
    refresh_sites_parquet_snapshots,
)
from data_access_service.config.config import DevConfig

logger = init_log(Config.get_config())

# Get the job ID from the environment variable
job_id = os.getenv("AWS_BATCH_JOB_ID")
logger.info(f"Job ID:{job_id}")

if not isinstance(Config.get_config(), DevConfig):
    # Get the index of the child job
    job_index = os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")
    if job_index is not None:
        logger.info(f"Job Index: { job_index }")

    # Only needed to describe the real Batch job; a local DevConfig run never
    # calls the Batch API, so skip requiring AWS region/credentials for it.
    client = boto3.client("batch")

    # Retrieve the job details
    response = client.describe_jobs(jobs=[job_id])
    jobs = response.get("jobs", [])
    if not jobs:
        raise ValueError(f"No job found with ID: {job_id}")

    job = jobs[0]

    # Extract parameters from the job details
    parameters = job.get("parameters")
    logger.info(f"Parameters: {parameters}")

    # Switch based on parameter call_type
    call_type = parameters.get("type")
else:
    # For local debug run only
    job_index = "1"
    call_type = os.getenv("AWS_BATCH_CALL_TYPE")
    parameters = {}

# A global app. Sites-parquet refresh doesn't touch the metadata catalog, so
# skip the memory-intensive metadata init for it — every other job type needs it.
api = None
if call_type != "refresh-sites-parquet":
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
    case "generate-pmtiles-for-parquet":
        # Optional single-UUID filter for local/debug (or Batch parameters).
        # Env wins only when parameters omit uuid so Batch jobs stay explicit.
        target_uuid = parameters.get("uuid") or os.getenv("PMTILES_TARGET_UUID")
        if target_uuid:
            logger.info("PMTiles generation restricted to uuid=%s", target_uuid)
        generate_pmtiles_for_all_parquets(api=api, uuid=target_uuid or None)
    case "generate-estimation-index-for-parquet":
        target_uuid = parameters.get("uuid") or os.getenv("ESTIMATION_TARGET_UUID")
        if target_uuid:
            logger.info(
                "Estimation index generation restricted to uuid=%s", target_uuid
            )
        generate_estimation_index_for_all_parquets(api=api, uuid=target_uuid or None)
    case "refresh-sites-parquet":
        refresh_sites_parquet_snapshots()
    case _:
        logger.error("Unknow call type", call_type)
