"""Remove pmtiles from S3 whose dataset is no longer in the catalog.

Called by generator.py at the start of a full batch run.
Input: the catalog as a list of (uuid, dataset).
Settings: s3_prefix and cleanup_dry_run in config.yaml.

Dry run on your machine, needs the AWS credentials of that environment:
    PROFILE=edge poetry run python -m data_access_service.batch.pmtiles.cleanup
It loads the catalog, logs one "Would delete" line per file, deletes nothing.
Dry run in the batch: set cleanup_dry_run: True in config-<env>.yaml.

1. Expected keys. Each parquet dataset in the catalog owns two:
       {s3_prefix}/{uuid}/{dataset}.pmtiles
       {s3_prefix}/{uuid}/{dataset}.metadata
2. Existing keys. Everything under {s3_prefix}/ in S3.
3. Outdated = existing but not expected.
   Real example from edge: the dataset
   mooring_estuarine_coastal_water_quality_monitoring_realtime_qc.parquet
   moved from uuid 613cd7ce-... to f3c16fdb-... (uuids shortened here).

       key in S3                                  expected?   result
       f3c16fdb-.../mooring_estuarine_...pmtiles   yes         keep
       613cd7ce-.../mooring_estuarine_...pmtiles   no          outdated
4. Safety stop, nothing is deleted when:
   - the catalog is empty
   - more than MAX_DELETE_RATIO of the folder is outdated
5. Delete the outdated keys, one log line per key.
   cleanup_dry_run True: log "Would delete ..." only, delete nothing.
"""

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper

config = Config.get_config()
logger = init_log(config)
aws = AWSHelper()

MAX_DELETE_RATIO = 0.5  # refuse to delete more than half of the folder


def remove_outdated_pmtiles(
    work: list[tuple[str, str]], dry_run: bool | None = None
) -> list[str]:
    """Delete S3 pmtiles of datasets not in ``work`` (the catalog).

    ``work`` is a list of (uuid, dataset_name). ``dry_run`` overrides the
    config value when given. Returns the deleted keys, or in dry run the
    keys it would delete.
    """
    pm_config = config.get_pmtiles_config()
    bucket = pm_config.bucket_name
    prefix = pm_config.s3_prefix
    if dry_run is None:
        dry_run = pm_config.cleanup_dry_run

    expected = catalog_keys(prefix, work)
    if not expected:
        logger.error("Cleanup skipped: catalog has no parquet datasets")
        return []

    existing = s3_keys(bucket, prefix)
    outdated = sorted(set(existing) - expected)
    logger.info(
        "Cleanup: %s object(s) in s3://%s/%s/, %s outdated, dry_run=%s",
        len(existing),
        bucket,
        prefix,
        len(outdated),
        dry_run,
    )
    if not outdated:
        return []

    if len(outdated) / len(existing) > MAX_DELETE_RATIO:
        logger.error(
            "Cleanup refused: %s of %s objects would be deleted, check the catalog",
            len(outdated),
            len(existing),
        )
        return []

    for key in outdated:
        logger.info(
            "%s s3://%s/%s", "Would delete" if dry_run else "Deleting", bucket, key
        )
    if dry_run:
        return outdated
    return delete_keys(bucket, outdated)


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
        errors = response.get("Errors", [])
        for error in errors:
            logger.error(
                "Failed to delete s3://%s/%s: %s %s",
                bucket,
                error.get("Key"),
                error.get("Code"),
                error.get("Message"),
            )
        failed = {error.get("Key") for error in errors}
        deleted += [key for key in chunk if key not in failed]
    logger.info(
        "Deleted %s object(s), %s failed", len(deleted), len(keys) - len(deleted)
    )
    return deleted


if __name__ == "__main__":
    # Local dry run, see the docstring at the top
    from data_access_service import API

    api = API()
    api.initialize_metadata()
    work = [
        (uuid, dname)
        for uuid, datasets in sorted(api.get_mapped_meta_data(uuid=None).items())
        for dname in datasets
        if dname.endswith(".parquet")
    ]
    remove_outdated_pmtiles(work, dry_run=True)
