"""remove_stale_pmtiles: delete S3 pmtiles whose dataset left the catalog.

Every test uses the same one-dataset catalog and describes S3 as a dict of
key -> upload time. The tests differ only in what S3 contains.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from data_access_service.batch.pmtiles import cleanup
from data_access_service.batch.pmtiles.cleanup import (
    pmtiles_s3_keys,
    remove_stale_pmtiles,
)

PREFIX = "portal/visualization"
BUCKET = "test-bucket"

RUN_STARTED = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
UPLOADED_LAST_WEEK = RUN_STARTED - timedelta(days=7)
UPLOADED_DURING_RUN = RUN_STARTED + timedelta(minutes=5)

# The catalog has exactly one dataset
CATALOG = [("uuid-a", "a.parquet")]
CURRENT_PMTILES, CURRENT_METADATA = pmtiles_s3_keys(PREFIX, "uuid-a", "a.parquet")
# A dataset that is no longer in the catalog
REMOVED_PMTILES, REMOVED_METADATA = pmtiles_s3_keys(PREFIX, "uuid-removed", "z.parquet")


def given_s3_contains(monkeypatch, objects, dry_run=False, delete_fails_for=()):
    """Stub config and S3 for one test. Returns the delete call spy."""
    monkeypatch.setattr(
        cleanup.config,
        "get_pmtiles_config",
        lambda: MagicMock(
            bucket_name=BUCKET, s3_prefix=PREFIX, cleanup_dry_run=dry_run
        ),
    )
    monkeypatch.setattr(cleanup.aws, "list_s3_objects", lambda bucket, prefix: objects)
    delete = MagicMock(return_value=list(delete_fails_for))
    monkeypatch.setattr(cleanup.aws, "delete_s3_objects", delete)
    return delete


class TestRemoveStalePmtiles:
    def test_deletes_files_of_datasets_not_in_catalog(self, monkeypatch):
        """A removed dataset's files go; the current dataset's files stay."""
        delete = given_s3_contains(
            monkeypatch,
            {
                CURRENT_PMTILES: UPLOADED_LAST_WEEK,
                CURRENT_METADATA: UPLOADED_LAST_WEEK,
                REMOVED_PMTILES: UPLOADED_LAST_WEEK,
                REMOVED_METADATA: UPLOADED_LAST_WEEK,
            },
        )

        deleted = remove_stale_pmtiles(CATALOG, RUN_STARTED)

        assert deleted == [REMOVED_METADATA, REMOVED_PMTILES]
        delete.assert_called_once_with(BUCKET, deleted)

    def test_keeps_files_uploaded_while_the_run_was_going(self, monkeypatch):
        """A file newer than the run may come from the API's PUT; never delete it."""
        delete = given_s3_contains(
            monkeypatch,
            {
                CURRENT_PMTILES: UPLOADED_LAST_WEEK,
                REMOVED_PMTILES: UPLOADED_DURING_RUN,
            },
        )

        assert remove_stale_pmtiles(CATALOG, RUN_STARTED) == []
        delete.assert_not_called()

    def test_does_nothing_when_catalog_is_empty(self, monkeypatch):
        """An empty catalog looks like a failed metadata load, not 'delete all'."""
        delete = given_s3_contains(monkeypatch, {REMOVED_PMTILES: UPLOADED_LAST_WEEK})

        assert remove_stale_pmtiles([], RUN_STARTED) == []
        delete.assert_not_called()

    def test_refuses_when_more_than_half_would_be_deleted(self, monkeypatch):
        """2 of 3 objects stale is over MAX_DELETE_RATIO, so nothing is deleted."""
        delete = given_s3_contains(
            monkeypatch,
            {
                CURRENT_PMTILES: UPLOADED_LAST_WEEK,
                REMOVED_PMTILES: UPLOADED_LAST_WEEK,
                REMOVED_METADATA: UPLOADED_LAST_WEEK,
            },
        )

        assert remove_stale_pmtiles(CATALOG, RUN_STARTED) == []
        delete.assert_not_called()

    def test_dry_run_reports_but_does_not_delete(self, monkeypatch):
        delete = given_s3_contains(
            monkeypatch,
            {
                CURRENT_PMTILES: UPLOADED_LAST_WEEK,
                CURRENT_METADATA: UPLOADED_LAST_WEEK,
                REMOVED_PMTILES: UPLOADED_LAST_WEEK,
            },
            dry_run=True,
        )

        assert remove_stale_pmtiles(CATALOG, RUN_STARTED) == [REMOVED_PMTILES]
        delete.assert_not_called()

    def test_keys_s3_failed_to_delete_are_not_reported_as_deleted(self, monkeypatch):
        given_s3_contains(
            monkeypatch,
            {
                CURRENT_PMTILES: UPLOADED_LAST_WEEK,
                CURRENT_METADATA: UPLOADED_LAST_WEEK,
                REMOVED_PMTILES: UPLOADED_LAST_WEEK,
                REMOVED_METADATA: UPLOADED_LAST_WEEK,
            },
            delete_fails_for=[REMOVED_PMTILES],
        )

        assert remove_stale_pmtiles(CATALOG, RUN_STARTED) == [REMOVED_METADATA]
