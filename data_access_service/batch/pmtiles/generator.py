"""The pmtiles batch job: map tiles for every parquet dataset.

How a Batch run goes:

1. The scheduled ``generate-pmtiles-for-parquet`` job (init) generates
   nothing itself: it submits one array job with a child per
   ``(uuid, key)``. The list goes to the children as the ``datasets``
   parameter.
2. Each ``generate-pmtiles-for-parquet-dataset`` child takes
   ``datasets[AWS_BATCH_JOB_ARRAY_INDEX]`` and makes its pmtiles, then its
   estimation index. Children run in parallel; Batch retries a failed one.
"""

import json
import os
import tempfile

from data_access_service import Config, init_log
from data_access_service.batch.estimation.generator import (
    generate_estimation_index_for_dataset,
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

# Job type of an array child, and the parameter holding its dataset list.
DATASET_JOB_TYPE = "generate-pmtiles-for-parquet-dataset"
DATASETS_PARAM = "datasets"


def list_parquet_datasets(
    api: BaseAPI, uuid: str | None = None
) -> list[tuple[str, str]]:
    """Every parquet ``(uuid, key)`` in the catalog, or only those of ``uuid``."""
    metadata_list = api.get_mapped_meta_data(uuid=None)
    datasets = [
        (k, dataset_name)
        for k, v in sorted(metadata_list.items())
        if uuid is None or k == uuid
        for dataset_name in v.keys()
        if dataset_name.endswith(".parquet")
    ]
    logger.info(
        "PMTiles: %s parquet dataset(s) for %s",
        len(datasets),
        f"uuid={uuid}" if uuid else "all UUIDs",
    )
    if not datasets:
        logger.warning("No parquet datasets found; nothing to generate")
    return datasets


def submit_pmtiles_array_job(
    api: BaseAPI,
    job_queue: str,
    job_definition: str,
    uuid: str | None = None,
) -> str | None:
    """Submit one array job with a child per parquet dataset.

    The children go to this init job's own queue and job definition.
    Returns the array job id, or None when there is no dataset.
    """
    datasets = list_parquet_datasets(api, uuid)
    if not datasets:
        return None

    # A child's log only shows its index; this maps it back to the dataset.
    for index, (k, dataset_name) in enumerate(datasets):
        logger.info("PMTiles child %d: uuid=%s dataset=%s", index, k, dataset_name)

    return aws.submit_a_job(
        job_name="generate-pmtiles-datasets",
        job_queue=job_queue,
        job_definition=job_definition,
        parameters={
            "type": DATASET_JOB_TYPE,
            DATASETS_PARAM: json.dumps(datasets),
        },
        array_size=len(datasets),
    )


def generate_pmtiles_for_child(
    api: BaseAPI, parameters: dict, job_index: str | None
) -> None:
    """Run one array child: the dataset at ``job_index`` in ``datasets``.

    ``uuid`` + ``key`` parameters run that dataset instead, for a run by hand.
    """
    uuid, dname = _pick_dataset(parameters, job_index)
    # This child only needs its own uuid; a smaller catalog is a smaller
    # baseline for the forked workers.
    api.release_memory_for_batch(
        keep_uuid=uuid, keep_suffix=".parquet", drop_instance=True
    )
    generate_pmtiles_for_dataset(api, uuid, dname)


def _pick_dataset(parameters: dict, job_index: str | None) -> tuple[str, str]:
    if parameters.get("key"):
        return parameters["uuid"], parameters["key"]

    datasets = json.loads(parameters[DATASETS_PARAM])
    # No index for a one-item list: Batch sends a plain job then, not an array.
    index = int(job_index) if job_index is not None else 0
    if not 0 <= index < len(datasets):
        raise ValueError(
            f"Array index {index} is out of range for {len(datasets)} dataset(s)"
        )
    uuid, dname = datasets[index]
    return uuid, dname


def generate_pmtiles_for_dataset(api: BaseAPI, uuid: str, dname: str) -> None:
    """PMTiles, then the estimation index, for one dataset.

    With ``pmtiles.config.use_fork_process`` each step runs in its own
    forked child, so its DuckDB / tippecanoe memory goes back to the OS when
    the child exits, and the index does not start on top of the memory the
    pmtiles step used. Fork reuses the loaded ``api`` (copy-on-write).
    Invariant: this parent only forks and waits; it must not open
    ``PmTileDuckDBClient`` itself.

    Raises when pmtiles fails, so Batch marks the child FAILED and retries
    it. A failed index only logs. The index still runs when pmtiles fails,
    as it did before.
    """
    pmtiles_config = config.get_pmtiles_config()
    if pmtiles_config.use_fork_process:
        ok = _generate_pmtiles_for_parquets_in_subprocess(api, uuid, dname)
    else:
        ok = _generate_pmtiles_for_parquets(api, uuid, dname)
    log_memory_usage(logger, f"after pmtiles for {dname}")

    if pmtiles_config.build_estimation_index:
        try:
            generate_estimation_index_for_dataset(api, uuid, dname)
        except Exception as e:
            logger.error(
                "Estimation index failed for uuid=%s dataset=%s: %s", uuid, dname, e
            )

    if not ok:
        raise RuntimeError(f"PMTiles failed for uuid={uuid} dataset={dname}")


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
    except Exception as e:
        logger.error(f"Pmtiles error processing dataset {uuid}, parquet {dname}: {e}")
        return False

    return True


def get_visualization_style(uuid: str, dname: str) -> PmtilesVisualizationStyle:
    # currently Hexagon is the default style. May need more styles in the future according to the uuid and dname
    return PmtilesVisualizationStyle.HEXAGONS
