"""Record data-preparation failures so the collection job can report them once.
"""

import io

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper


class PreparationFailedError(RuntimeError):
    """One or more data-preparation children of a request failed."""


def preparation_failure_prefix(master_job_id: str) -> str:
    return f"{master_job_id}/errors/"


def record_preparation_failure(
    master_job_id: str, job_index: str, error: Exception
) -> bool:
    config = Config.get_config()
    log = init_log(config)
    content = f"job_index={job_index}\nerror={type(error).__name__}\n"
    try:
        AWSHelper().upload_fileobj_to_s3(
            io.BytesIO(content.encode("utf-8")),
            config.get_subsetting_bucket_name(),
            f"{preparation_failure_prefix(master_job_id)}{job_index}.txt",
        )
        return True
    except Exception:
        log.exception("Failed to record preparation failure for child %s", job_index)
        return False


def raise_if_preparation_failed(
    aws: AWSHelper, bucket_name: str, master_job_id: str
) -> None:
    markers = aws.get_s3_keys(bucket_name, preparation_failure_prefix(master_job_id))
    if markers:
        raise PreparationFailedError(
            f"{len(markers)} data preparation job(s) failed for {master_job_id}: "
            f"{sorted(markers)}"
        )
