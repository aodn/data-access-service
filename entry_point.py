import json
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
from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.config.config import DevConfig

logger = init_log(Config.get_config())
config = Config.get_config()
# Get the job ID from the environment variable
job_id = os.getenv("AWS_BATCH_JOB_ID")
logger.info(f"Job ID:{job_id}")


def _parse_local_job_parameters(raw: str | None) -> dict:
    """Turn LOCAL_JOBS_PARAM into the same dict Batch puts on the job.

    Accepts the parameters object, a describe_jobs job, or {\"jobs\": [...]}.
    Nested multi_polygon / date_ranges objects are JSON-stringified like Batch.
    """
    if not raw or not raw.strip():
        return {}
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "LOCAL_JOBS_PARAM must be one-line JSON. IntelliJ env values "
            "cannot contain raw newlines."
        ) from exc
    if isinstance(loaded, str):
        loaded = json.loads(loaded)
    if isinstance(loaded, dict) and isinstance(loaded.get("jobs"), list):
        loaded = loaded["jobs"][0] if loaded["jobs"] else {}
    if isinstance(loaded, dict) and isinstance(loaded.get("parameters"), dict):
        loaded = loaded["parameters"]
    if not isinstance(loaded, dict):
        raise ValueError("LOCAL_JOBS_PARAM must be a JSON object of job parameters")
    for key in (
        Parameters.MULTI_POLYGON.value,
        Parameters.DATE_RANGES.value,
    ):
        if key in loaded and not isinstance(loaded[key], str):
            loaded[key] = json.dumps(loaded[key])
    if not loaded.get(Parameters.OUTPUT_FORMAT.value):
        loaded[Parameters.OUTPUT_FORMAT.value] = "netcdf"
    if not loaded.get(Parameters.TYPE.value):
        call_type = os.getenv("AWS_BATCH_CALL_TYPE")
        if call_type:
            loaded[Parameters.TYPE.value] = call_type
    return loaded


if not isinstance(config, DevConfig):
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
    if not jobs or len(jobs) == 0:
        raise ValueError(f"No job found with ID: {job_id}")

    job = jobs[0]

    # Extract parameters from the job details
    parameters = job.get("parameters")
    logger.info(f"Parameters: {parameters}")

    # Switch based on parameter call_type
    call_type = parameters.get("type")
else:
    # For local debug run only
    job_index = os.getenv("AWS_BATCH_JOB_ARRAY_INDEX", "1")
    # LOCAL_JOBS_PARAM: JSON object of Batch job parameters, sample
    # {
    #   "type":"sub-setting-data-preparation",
    #   "uuid":"<your-uuid>",
    #   "start_date":"07-2010",
    #   "end_date":"06-2011",
    #   "recipient":"you@example.com",
    #   "multi_polygon":"{\"type\":\"MultiPolygon\",\"coordinates\":[[[[38.22656250000031,55.578344672182],[60.02343749999969,55.578344672182],[60.02343749999969,61.77312286453116],[38.22656250000031,61.77312286453116],[38.22656250000031,55.578344672182]]]]}",
    #   "date_ranges":"{\"13\": [\"2010-07-01 00:00:00.000000000\", \"2011-06-30 23:59:59.999999999\"]}",
    #   "master_job_id":"local-debug",
    #   "intermediate_output_folder":"/tmp/local-subset"
    # }
    raw = os.getenv("LOCAL_JOBS_PARAM") or os.getenv("LOCAL_JOB_PARAM")
    parameters = _parse_local_job_parameters(raw)
    call_type = parameters.get("type") or os.getenv("AWS_BATCH_CALL_TYPE")
    logger.info(f"Job Index: {job_index}")
    logger.info(f"Parameters: {parameters}")
    if call_type in (
        "sub-setting",
        "sub-setting-data-preparation",
        "sub-setting-data-collection",
    ):
        required = (
            Parameters.UUID.value,
            Parameters.START_DATE.value,
            Parameters.END_DATE.value,
            Parameters.RECIPIENT.value,
            Parameters.MULTI_POLYGON.value,
        )
        missing = [key for key in required if key not in parameters]
        if call_type != "sub-setting":
            for key in (
                Parameters.DATE_RANGES.value,
                Parameters.MASTER_JOB_ID.value,
                Parameters.INTERMEDIATE_OUTPUT_FOLDER.value,
            ):
                if key not in parameters:
                    missing.append(key)
        if missing:
            raise ValueError(
                "LOCAL_JOBS_PARAM is missing keys required by get_subset_request: "
                f"{missing}. Got keys: {sorted(parameters)}"
            )

match call_type:
    case "sub-setting":
        api = API()
        api.initialize_metadata()
        subsetting.init(api, job_id_of_init=job_id, parameters=parameters)
    case "sub-setting-data-preparation":
        """
        Please take noted that the parameters in each call are different, the batch will call the
        first job init, and init job will add some parameter before calling the prepare_data job
        """
        api = API()
        api.initialize_metadata()
        subsetting.prepare_data(api, job_index=job_index, parameters=parameters)
    case "sub-setting-data-collection":
        subsetting.collect_data(parameters=parameters)
    case "generate-pmtiles-for-parquet":
        api = API()
        api.initialize_metadata()
        # Optional single-UUID filter for local/debug (or Batch parameters).
        # Env wins only when parameters omit uuid so Batch jobs stay explicit.
        target_uuid = parameters.get("uuid") or os.getenv("PMTILES_TARGET_UUID")
        if target_uuid:
            logger.info("PMTiles generation restricted to uuid=%s", target_uuid)
        generate_pmtiles_for_all_parquets(api=api, uuid=target_uuid or None)
    case "generate-estimation-index-for-parquet":
        api = API()
        api.initialize_metadata()
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
