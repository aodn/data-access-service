"""Record data-preparation failures so the collection job can report them once.

A failed array child would fail the whole array job, and AWS Batch then fails
the dependent collection job without running it, so nobody could email the
user. Instead a failing child writes a marker and exits successfully; the
collection job finds the marker and raises, which sends one error email.
"""

import io

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper


class PreparationFailedError(RuntimeError):
    """One or more data-preparation children of a request failed."""


def preparation_failure_prefix(master_job_id: str) -> str:
    # Outside get_s3_temp_folder_name(), so the collector's folder listing
    # never treats a marker as dataset output.
    return f"{master_job_id}/errors/"


def record_preparation_failure(
    master_job_id: str, job_index: str, error: Exception
) -> bool:
    """Write the marker for this child. Returns False if it could not be
    written, in which case the caller should fail the child instead."""
    config = Config.get_config()
    log = init_log(config)
    # Keep the marker short: the traceback is already in the job log, and the
    # subsetting bucket also holds the user-facing download files.
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
