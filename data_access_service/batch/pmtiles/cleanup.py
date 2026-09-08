"""Remove leftover pmtiles from S3 after a dataset is removed from the catalog.

Runs once at the end of a full batch run (see generator.py). To disable, set
``pmtiles.config.cleanup_stale_pmtiles: False``.
"""

from datetime import datetime

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()

# Safety guard: if more than 50% of files look stale, delete nothing
MAX_DELETE_RATIO = 0.5


def pmtiles_s3_keys(s3_prefix: str, uuid: str, dname: str) -> tuple[str, str]:
    """The two S3 keys of one dataset: the .pmtiles file and its .metadata file."""
    base = f"{s3_prefix}/{uuid}/{dname}"
    return f"{base}.pmtiles", f"{base}.metadata"


def remove_stale_pmtiles(
    work: list[tuple[str, str]], started_at: datetime
) -> list[str]:
    """Delete pmtiles in S3 for datasets that are not in ``work`` (the catalog).

    Files uploaded after ``started_at`` are kept, they may be new uploads from
    the API. Returns the deleted keys (in dry run: the keys it would delete).
    """
    pm_config = config.get_pmtiles_config()
    bucket = pm_config.bucket_name
    # Add a slash so we only list files inside this folder
    prefix = f"{pm_config.s3_prefix}/"

    # Step 1: which files should exist, based on the catalog
    expected = {
        key
        for uuid, dname in work
        for key in pmtiles_s3_keys(pm_config.s3_prefix, uuid, dname)
    }
    logger.info(
        "Stale pmtiles cleanup: s3://%s/%s, %s dataset(s) in catalog, "
        "keep objects modified after %s, dry_run=%s",
        bucket,
        prefix,
        len(work),
        started_at.isoformat(),
        pm_config.cleanup_dry_run,
    )
    # Safety guard: an empty catalog usually means metadata failed to load
    if not expected:
        logger.warning("Stale pmtiles cleanup skipped: catalog has no parquet datasets")
        return []

    # Step 2: which files actually exist in S3, and when they were uploaded
    objects = aws.list_s3_objects(bucket, prefix)

    # Step 3: stale = in S3, not in the catalog, and uploaded before this run
    stale = sorted(
        key
        for key, modified in objects.items()
        if key not in expected and modified < started_at
    )
    # For the log: files not in the catalog but kept because they are new
    recent = sum(
        1
        for key, modified in objects.items()
        if key not in expected and modified >= started_at
    )
    logger.info(
        "Listed %s object(s): %s stale, %s kept as newer than this run",
        len(objects),
        len(stale),
        recent,
    )
    if not stale:
        return []

    # Step 4: safety guard, stop if too much would be deleted
    if len(stale) / len(objects) > MAX_DELETE_RATIO:
        logger.error(
            "Stale pmtiles cleanup refused: %s of %s objects would be deleted "
            "(limit %.0f%%); check the catalog",
            len(stale),
            len(objects),
            MAX_DELETE_RATIO * 100,
        )
        return []

    # Step 5: log every file, then delete them (unless dry run)
    action = "Would delete" if pm_config.cleanup_dry_run else "Deleting"
    for key in stale:
        logger.info("%s stale pmtiles s3://%s/%s", action, bucket, key)
    if pm_config.cleanup_dry_run:
        logger.info("Dry run: %s stale object(s) left in place", len(stale))
        return stale
    # Return only the files S3 really deleted
    failed = set(aws.delete_s3_objects(bucket, stale))
    deleted = [key for key in stale if key not in failed]
    logger.info("Deleted %s stale object(s), %s failed", len(deleted), len(failed))
    return deleted
