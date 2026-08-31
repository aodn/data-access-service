"""Run the estimation-index build for one or every parquet dataset.

Mirrors :mod:`data_access_service.batch.pmtiles.generator`: datasets run one at
a time, each in a forked child so DuckDB's memory goes back to the OS, and both
output files are uploaded beside the pmtiles of the same dataset.
"""

import os
import tempfile
import threading

from data_access_service import Config, init_log
from data_access_service.batch.estimation.index_builder import (
    EmptyDatasetError,
    EstimationIndexBuilder,
)
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.api import BaseAPI
from data_access_service.utils.memory_utils import log_memory_usage

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()

# Same reason as PMTiles: one run per process, because the build owns the
# process-global PmTileDuckDBClient connection and tears it down when finished.
_generation_lock = threading.Lock()


class EstimationIndexGenerationInProgressError(RuntimeError):
    """Raised when an index build is requested while another is running."""


def generate_estimation_index_for_all_parquets(api: BaseAPI, uuid: str | None = None):
    """Build the estimation index for every parquet dataset in the catalog.

    Args:
        api: Initialized API with metadata loaded.
        uuid: Optional catalog UUID. When set, only that UUID's parquet
            datasets are processed (useful for backfills and local runs).
    """
    metadata_list = api.get_mapped_meta_data(uuid=None)

    work: list[tuple[str, str]] = []
    for k, v in sorted(metadata_list.items()):
        if uuid is not None and k != uuid:
            continue
        for dataset_name in v.keys():
            if dataset_name.endswith(".parquet"):
                work.append((k, dataset_name))

    logger.info(
        "Estimation index batch for %s (%s parquet dataset(s))",
        f"uuid={uuid}" if uuid else "all UUIDs",
        len(work),
    )
    if not work:
        logger.warning("No parquet datasets to index; nothing to generate")
        return

    use_fork = config.get_estimation_config().use_fork_process
    logger.info(
        "Estimation index batch process isolation: use_fork_process=%s", use_fork
    )

    for k, dataset_name in work:
        if use_fork:
            ok = _build_in_subprocess(api, k, dataset_name)
            after_label = f"after child for {dataset_name}"
        else:
            ok = build_and_upload_estimation_index(api, k, dataset_name)
            after_label = f"after in-process run for {dataset_name}"
        if not ok:
            logger.error(
                "Estimation index worker failed for uuid=%s dataset=%s",
                k,
                dataset_name,
            )
        log_memory_usage(logger, after_label)


def _build_in_subprocess(api: BaseAPI, uuid: str, dname: str) -> bool:
    """Fork a worker for one dataset; wait until it exits."""
    logger.info(
        "Forking estimation index worker parent_pid=%s uuid=%s dataset=%s",
        os.getpid(),
        uuid,
        dname,
    )
    pid = os.fork()
    if pid == 0:
        # Child: never return into the parent loop.
        try:
            ok = build_and_upload_estimation_index(api, uuid, dname)
            log_memory_usage(logger, f"worker exit ({dname})")
            os._exit(0 if ok else 1)
        except BaseException:
            logger.exception(
                "Estimation index worker crashed uuid=%s dataset=%s", uuid, dname
            )
            os._exit(1)

    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
        if code == 0:
            logger.info(
                "Estimation index worker finished successfully for uuid=%s dataset=%s",
                uuid,
                dname,
            )
            return True
        logger.error(
            "Estimation index worker exit code=%s for uuid=%s dataset=%s",
            code,
            uuid,
            dname,
        )
        return False

    termsig = os.WTERMSIG(status) if os.WIFSIGNALED(status) else None
    logger.error(
        "Estimation index worker signaled status=%s termsig=%s for uuid=%s dataset=%s",
        status,
        termsig,
        uuid,
        dname,
    )
    return False


def generate_estimation_index_for_parquets(api: BaseAPI, uuid: str, dname: str) -> bool:
    """One dataset, rejecting the call when another build is already running."""
    if not _generation_lock.acquire(blocking=False):
        raise EstimationIndexGenerationInProgressError(
            f"Another estimation index generation is already running in this "
            f"process; rejected request for uuid {uuid}, dataset {dname}. Retry later."
        )
    try:
        return build_and_upload_estimation_index(api, uuid, dname)
    finally:
        _generation_lock.release()


def build_and_upload_estimation_index(api: BaseAPI, uuid: str, dname: str) -> bool:
    """Build both files in a temp dir and upload them to the portal-data bucket.

    Also called from the pmtiles job (right after that dataset's pmtiles are
    uploaded), so it never raises: a failed index must not fail the pmtiles run.
    """
    try:
        logger.info(
            f"Start generating estimation index for uuid: {uuid}, dataset: {dname}"
        )
        estimation_config = config.get_estimation_config()

        # Temp dir so a failed run leaves nothing behind on the batch host.
        with tempfile.TemporaryDirectory() as tempdirname:
            builder = EstimationIndexBuilder(
                work_dir=tempdirname, uuid=uuid, dataset_name=dname, api=api
            )
            index_path, metadata_path = builder.build()

            s3_dir = f"{estimation_config.s3_prefix}/{uuid}"
            aws.upload_file_to_s3(
                index_path,
                estimation_config.bucket_name,
                f"{s3_dir}/{dname}.parquet",
            )
            aws.upload_file_to_s3(
                metadata_path,
                estimation_config.bucket_name,
                f"{s3_dir}/{dname}.metadata",
            )
            logger.info(
                "Estimation index of dataset %s, uuid %s uploaded to s3://%s/%s",
                dname,
                uuid,
                estimation_config.bucket_name,
                s3_dir,
            )
    except EmptyDatasetError as e:
        # Nothing to index is not a failure of the job.
        logger.warning("Estimation index skipped for %s/%s: %s", uuid, dname, e)
        return False
    except Exception as e:
        logger.error(
            f"Estimation index error processing dataset {uuid}, parquet {dname}: {e}"
        )
        return False

    return True
