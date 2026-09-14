"""remove_outdated_pmtiles: delete S3 pmtiles whose dataset left the catalog.

Every test uses the same one-dataset catalog and only changes what S3 contains.
"""

from unittest.mock import MagicMock

from data_access_service.batch.pmtiles import cleanup
from data_access_service.batch.pmtiles.cleanup import remove_outdated_pmtiles

BUCKET = "test-bucket"

# The catalog has exactly one dataset
CATALOG = [("uuid-a", "a.parquet")]
CURRENT_PMTILES = "portal/visualization/uuid-a/a.parquet.pmtiles"
CURRENT_METADATA = "portal/visualization/uuid-a/a.parquet.metadata"
# A dataset that is no longer in the catalog
REMOVED_PMTILES = "portal/visualization/uuid-removed/z.parquet.pmtiles"
REMOVED_METADATA = "portal/visualization/uuid-removed/z.parquet.metadata"


def stub_s3(monkeypatch, keys, dry_run=False, delete_errors=()):
    """Fake settings and S3 content. Returns the S3 delete call spy."""
    monkeypatch.setattr(
        cleanup.config,
        "get_pmtiles_config",
        lambda: MagicMock(
            bucket_name=BUCKET,
            s3_prefix="portal/visualization",
            cleanup_dry_run=dry_run,
        ),
    )
    monkeypatch.setattr(cleanup.aws, "list_all_s3_objects", lambda b, p: list(keys))
    delete = MagicMock(return_value={"Errors": [{"Key": k} for k in delete_errors]})
    monkeypatch.setattr(cleanup.aws, "s3", MagicMock(delete_objects=delete))
    return delete


def test_delete_outdated(monkeypatch):
    # The removed dataset's files go, the current dataset's files stay
    delete = stub_s3(
        monkeypatch,
        [CURRENT_PMTILES, CURRENT_METADATA, REMOVED_PMTILES, REMOVED_METADATA],
    )

    deleted = remove_outdated_pmtiles(CATALOG)

    assert sorted(deleted) == sorted([REMOVED_PMTILES, REMOVED_METADATA])
    delete.assert_called_once()
    sent = delete.call_args.kwargs["Delete"]["Objects"]
    assert sorted(o["Key"] for o in sent) == sorted(deleted)


def test_empty_catalog(monkeypatch):
    # An empty catalog looks like a failed metadata load, not "delete all"
    delete = stub_s3(monkeypatch, [REMOVED_PMTILES])

    assert remove_outdated_pmtiles([]) == []
    delete.assert_not_called()


def test_ratio_guard(monkeypatch):
    # 2 of 3 files outdated is over MAX_DELETE_RATIO, so nothing is deleted
    delete = stub_s3(monkeypatch, [CURRENT_PMTILES, REMOVED_PMTILES, REMOVED_METADATA])

    assert remove_outdated_pmtiles(CATALOG) == []
    delete.assert_not_called()


def test_dry_run(monkeypatch):
    # Dry run returns what it would delete but never calls S3 delete
    delete = stub_s3(
        monkeypatch, [CURRENT_PMTILES, CURRENT_METADATA, REMOVED_PMTILES], dry_run=True
    )

    assert remove_outdated_pmtiles(CATALOG) == [REMOVED_PMTILES]
    delete.assert_not_called()


def test_delete_error(monkeypatch):
    # A key S3 failed to delete is not reported as deleted
    stub_s3(
        monkeypatch,
        [CURRENT_PMTILES, CURRENT_METADATA, REMOVED_PMTILES, REMOVED_METADATA],
        delete_errors=[REMOVED_PMTILES],
    )

    assert remove_outdated_pmtiles(CATALOG) == [REMOVED_METADATA]


def test_batch_limit(monkeypatch):
    # S3 takes at most 1000 keys per delete call, so 1001 keys need two calls
    outdated = [f"portal/visualization/old/{i}.parquet.pmtiles" for i in range(1001)]
    # Enough current files so the outdated share stays under MAX_DELETE_RATIO
    current = [f"portal/visualization/uuid-a/{i}.parquet.pmtiles" for i in range(1001)]
    catalog = [("uuid-a", f"{i}.parquet") for i in range(1001)]
    delete = stub_s3(monkeypatch, outdated + current)

    deleted = remove_outdated_pmtiles(catalog)

    assert len(deleted) == 1001
    first, second = delete.call_args_list
    assert len(first.kwargs["Delete"]["Objects"]) == 1000
    assert len(second.kwargs["Delete"]["Objects"]) == 1
