"""Remove pmtiles from S3 whose dataset is no longer in the catalog.

Called once at the start of a full pmtiles batch run, see generator.py.
Settings come from config.yaml, pmtiles.config: s3_prefix and cleanup_dry_run.
"""

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()

MAX_DELETE_RATIO = 0.5  # refuse to delete more than half of the folder


def remove_stale_pmtiles(work: list[tuple[str, str]]) -> list[str]:
    """Delete S3 pmtiles of datasets not in ``work`` (the catalog).

    ``work`` is a list of (uuid, dataset_name). Returns the deleted keys,
    or in dry run the keys it would delete.
    """
    pm_config = config.get_pmtiles_config()
    bucket = pm_config.bucket_name
    prefix = pm_config.s3_prefix
    dry_run = pm_config.cleanup_dry_run

    expected = catalog_keys(prefix, work)
    if not expected:
        logger.error("Cleanup skipped: catalog has no parquet datasets")
        return []

    existing = s3_keys(bucket, prefix)
    stale = sorted(set(existing) - expected)
    logger.info(
        "Cleanup: %s object(s) in s3://%s/%s/, %s stale, dry_run=%s",
        len(existing),
        bucket,
        prefix,
        len(stale),
        dry_run,
    )
    if not stale:
        return []

    if len(stale) / len(existing) > MAX_DELETE_RATIO:
        logger.error(
            "Cleanup refused: %s of %s objects would be deleted, check the catalog",
            len(stale),
            len(existing),
        )
        return []

    for key in stale:
        logger.info(
            "%s s3://%s/%s", "Would delete" if dry_run else "Deleting", bucket, key
        )
    if dry_run:
        return stale
    return delete_keys(bucket, stale)


def catalog_keys(prefix: str, work: list[tuple[str, str]]) -> set[str]:
    """The .pmtiles and .metadata key of every dataset in the catalog."""
    return {
        f"{prefix}/{uuid}/{dname}{ext}"
        for uuid, dname in work
        for ext in (".pmtiles", ".metadata")
    }


def s3_keys(bucket: str, prefix: str) -> list[str]:
    """Every key inside the pmtiles folder in S3."""
    return aws.list_all_s3_objects(bucket, f"{prefix}/")


def delete_keys(bucket: str, keys: list[str]) -> list[str]:
    """Delete in batches of 1000 (S3 limit). Returns the keys S3 really deleted."""
    deleted = []
    for start in range(0, len(keys), 1000):
        chunk = keys[start : start + 1000]
        response = aws.s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )
        failed = {error.get("Key") for error in response.get("Errors", [])}
        for key in failed:
            logger.error("Failed to delete s3://%s/%s", bucket, key)
        deleted += [key for key in chunk if key not in failed]
    logger.info(
        "Deleted %s object(s), %s failed", len(deleted), len(keys) - len(deleted)
    )
    return deleted
