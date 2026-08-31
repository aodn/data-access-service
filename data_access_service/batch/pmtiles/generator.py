import os
import tempfile
import threading

from data_access_service import Config, init_log
from data_access_service.batch.estimation.generator import (
    build_and_upload_estimation_index,
)
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.api import BaseAPI
from data_access_service.utils.memory_utils import log_memory_usage

from .processors.hexbin_processor import HexbinProcessor
from ...models.pmtiles_types import (
    PmtilesVisualizationStyle,
)

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()

# PMTiles generation must not run concurrently within one process: each run
# uses the process-global PmTileDuckDBClient connection and tears it down via
# shutdown() when finished, which would kill any other run still mid-query
_generation_lock = threading.Lock()


class PmtilesGenerationInProgressError(RuntimeError):
    """Raised when a PMTiles generation is requested while another is running."""


def generate_pmtiles_for_all_parquets(api: BaseAPI, uuid: str | None = None):
    """Generate PMTiles for every parquet dataset in the catalog.

    Process isolation is controlled by ``pmtiles.config.use_fork_process``:

    * **True (default):** each dataset runs in a forked child so DuckDB /
      tippecanoe allocations are returned to the OS when the child exits.
      Fork (not re-exec) reuses the already-initialized ``api`` via
      copy-on-write — no metadata reload per dataset. Invariant: the parent
      must not open ``PmTileDuckDBClient`` before forking; the child creates
      its own process-global DuckDB connection.
    * **False:** each dataset runs in the main app process (no ``os.fork``).
      Useful for local debug or when an APM agent cannot tolerate forking.

    Datasets always run sequentially so only one heavy worker is live at a
    time.

    Args:
        api: Initialized API with metadata loaded.
        uuid: Optional catalog UUID. When set, only parquet datasets for that
            UUID are processed (useful for local/debug runs of a single product).
    """
    metadata_list = api.get_mapped_meta_data(uuid=None)

    # Materialise the work list before trimming so we do not depend on the
    # full cached map (zarr entries etc.) remaining in the parent.
    work: list[tuple[str, str]] = []
    for k, v in sorted(metadata_list.items()):
        if uuid is not None and k != uuid:
            continue
        for dataset_name in v.keys():
            if dataset_name.endswith(".parquet"):
                work.append((k, dataset_name))

    if uuid is not None:
        logger.info(
            "PMTiles batch restricted to uuid=%s (%s parquet dataset(s))",
            uuid,
            len(work),
        )
        if not work:
            logger.warning(
                "No parquet datasets found for uuid=%s; nothing to generate",
                uuid,
            )
            return
    else:
        logger.info(
            "PMTiles batch for all UUIDs (%s parquet dataset(s))",
            len(work),
        )

    use_fork = config.get_pmtiles_config().use_fork_process
    logger.info(
        "PMTiles batch process isolation: use_fork_process=%s",
        use_fork,
    )

    # Drop full raw schemas / non-parquet metadata so the parent (and COW
    # fork children) start each dataset with a smaller baseline RSS.
    api.release_memory_for_pmtiles_batch()

    for k, dataset_name in work:
        if use_fork:
            ok = _generate_pmtiles_for_parquets_in_subprocess(api, k, dataset_name)
            after_label = f"after child for {dataset_name}"
        else:
            ok = _generate_pmtiles_for_parquets(api, k, dataset_name)
            after_label = f"after in-process run for {dataset_name}"
        if not ok:
            logger.error(
                "PMTiles worker failed for uuid=%s dataset=%s",
                k,
                dataset_name,
            )
        log_memory_usage(logger, after_label)


def _generate_pmtiles_for_parquets_in_subprocess(
    api: BaseAPI, uuid: str, dname: str
) -> bool:
    """Fork a worker for one dataset; wait until it exits.

    The child inherits the parent's initialized ``api`` (no catalog reload).
    Returns True when the child exits with code 0. Uses ``os._exit`` in the
    child so parent atexit handlers do not run twice.

    TODO: The reason we need this is some memory is not free correctly and due
    to short timeline, it is easier to fork a new process and kill it at end
    which make sure memory reclaim
    """
    logger.info(
        "Forking PMTiles worker parent_pid=%s uuid=%s dataset=%s",
        os.getpid(),
        uuid,
        dname,
    )
    pid = os.fork()
    if pid == 0:
        # Child: never return into the parent loop.
        try:
            ok = _generate_pmtiles_for_parquets(api, uuid, dname)
            log_memory_usage(logger, f"worker exit ({dname})")
            os._exit(0 if ok else 1)
        except BaseException:
            logger.exception("PMTiles worker crashed uuid=%s dataset=%s", uuid, dname)
            os._exit(1)

    # Parent
    _, status = os.waitpid(pid, 0)
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
        if code == 0:
            logger.info(
                "PMTiles worker finished successfully for uuid=%s dataset=%s",
                uuid,
                dname,
            )
            return True
        logger.error(
            "PMTiles worker exit code=%s for uuid=%s dataset=%s",
            code,
            uuid,
            dname,
        )
        return False

    # Raw wait status (e.g. 139 = SIGSEGV + core): decode for operators.
    termsig = os.WTERMSIG(status) if os.WIFSIGNALED(status) else None
    coredump = os.WCOREDUMP(status) if hasattr(os, "WCOREDUMP") else False
    logger.error(
        "PMTiles worker signaled status=%s termsig=%s coredump=%s "
        "for uuid=%s dataset=%s "
        "(common: 11=SIGSEGV native crash, 9=SIGKILL often OOM killer)",
        status,
        termsig,
        coredump,
        uuid,
        dname,
    )
    return False


def generate_pmtiles_for_parquets(api: BaseAPI, uuid: str, dname: str) -> bool:
    # Fail fast instead of queueing: a queued run would hold a worker (and its
    # SSE connection) for potentially an hour, and callers can simply retry.
    if not _generation_lock.acquire(blocking=False):
        raise PmtilesGenerationInProgressError(
            f"Another PMTiles generation is already running in this process; "
            f"rejected request for uuid {uuid}, dataset {dname}. Retry later."
        )
    try:
        return _generate_pmtiles_for_parquets(api, uuid, dname)
    finally:
        _generation_lock.release()


def _generate_pmtiles_for_parquets(api: BaseAPI, uuid: str, dname: str) -> bool:

    try:
        logger.info(f"Start generating PMTiles for uuid: {uuid}, dataset: {dname}")

        # Do everything in a temp directory to avoid filling up disk space.
        # The temp directory and all its contents will be automatically deleted after the with block.
        with tempfile.TemporaryDirectory() as tempdirname:

            vis_style = get_visualization_style(uuid=uuid, dname=dname)

            if vis_style == PmtilesVisualizationStyle.HEXAGONS:
                logger.info("Visualization style: HEXAGONS")
                hex_processor = HexbinProcessor(
                    work_dir=tempdirname, uuid=uuid, dataset_name=dname, api=api
                )
                logger.info("Hexbin Processor has been initialized.")
                pmtiles_path, metadata_path = hex_processor.process()
                # TODO: please use functions like is_local_pmtiles_valid() in pmtiles_util to verify the new generated pmtiles file
                #  is valid or not before uploading to S3. We don't want to upload an invalid pmtiles file to S3 and cause errors
                # [Raymond] Is the function is_local_pmtiles_valid() in pmtiles_util.py reliable? Seems not
                bucket = config.get_pmtiles_config().bucket_name
                s3_dir = f"portal/visualization/{uuid}"
                aws.upload_file_to_s3(
                    pmtiles_path,
                    bucket,
                    f"{s3_dir}/{dname}.pmtiles",
                )
                logger.info(
                    f"Pmtiles file of dataset {dname}, uuid {uuid} uploaded to S3."
                )
                aws.upload_file_to_s3(
                    metadata_path,
                    bucket,
                    f"{s3_dir}/{dname}.metadata",
                )
                logger.info(
                    f"Metadata file of dataset {dname}, uuid {uuid} uploaded to S3."
                )

            # Build the estimate index for the same dataset while we are here:
            # this child already has the api, the DuckDB session and the
            # dataset name, and it is killed afterwards so the memory goes
            # back either way. Never fails the pmtiles run.
            if config.get_estimation_config().build_with_pmtiles:
                build_and_upload_estimation_index(api, uuid, dname)
    except Exception as e:
        logger.error(f"Pmtiles error processing dataset {uuid}, parquet {dname}: {e}")
        return False

    return True


def get_visualization_style(uuid: str, dname: str) -> PmtilesVisualizationStyle:
    # currently Hexagon is the default style. May need more styles in the future according to the uuid and dname
    return PmtilesVisualizationStyle.HEXAGONS
