import re
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from data_access_service import init_log
from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.config.config import Config
from data_access_service.core.api import API
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.routes.auth import api_key_auth
from data_access_service.core.routes.helpers import require_api_ready
from data_access_service.utils.date_time_utils import time_it

router = APIRouter()

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()

# Batch call type handled by entry_point.py
PMTILES_JOB_TYPE = "generate-pmtiles-for-parquet"

# AWS Batch job names accept letters, digits, hyphen and underscore only.
_NOT_ALLOWED_IN_JOB_NAME = re.compile(r"[^A-Za-z0-9_-]")
_MAX_JOB_NAME_LENGTH = 128


class PmtilesJobSubmitted(BaseModel):
    """What the caller gets back after the job is queued."""

    job_id: str
    job_name: str
    job_queue: str
    uuid: str
    message: str


def _job_name(uuid: str) -> str:
    name = _NOT_ALLOWED_IN_JOB_NAME.sub("-", f"pmtiles-{uuid}")
    return name[:_MAX_JOB_NAME_LENGTH]


@router.put(
    "/pmtiles/{uuid}",
    dependencies=[Depends(api_key_auth)],
    status_code=HTTPStatus.ACCEPTED,  # 202
    response_model=PmtilesJobSubmitted,
)
@time_it
def create_pmtiles(uuid: str, api_instance: API = Depends(require_api_ready)):
    """Queue an AWS Batch job that generates the pmtiles for every parquet
    dataset of one uuid.

    The generation itself is heavy (DuckDB + tippecanoe, up to about an hour),
    so it must not run inside this always-on API process. Returns straight
    away with the Batch job id.
    """
    # Catch a wrong uuid here: the job would otherwise start, find no parquet
    # dataset and exit without generating anything.
    datasets = api_instance.get_mapped_meta_data(uuid)
    if not any(name.endswith(".parquet") for name in datasets):
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,  # 404
            detail=f"No parquet dataset found for uuid '{uuid}'.",
        )

    job_queue = config.get_job_queue_name()
    job_definition = config.get_job_definition_name()
    if not job_queue or not job_definition:
        raise HTTPException(
            status_code=HTTPStatus.SERVICE_UNAVAILABLE,  # 503
            detail=("AWS Batch is not configured for this profile."),
        )

    job_name = _job_name(uuid)
    job_id = aws.submit_a_job(
        job_name=job_name,
        job_queue=job_queue,
        job_definition=job_definition,
        parameters={
            Parameters.TYPE.value: PMTILES_JOB_TYPE,
            Parameters.UUID.value: uuid,
        },
    )
    logger.info("Submitted PMTiles job id=%s name=%s uuid=%s", job_id, job_name, uuid)
    return PmtilesJobSubmitted(
        job_id=job_id,
        job_name=job_name,
        job_queue=job_queue,
        uuid=uuid,
        message=(
            "PMTiles generation submitted to AWS Batch. Check the progress with "
            f"'aws batch describe-jobs --jobs {job_id}', or in the Batch console."
        ),
    )
