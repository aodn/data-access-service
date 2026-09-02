"""AWS Batch entrypoint work for refreshing the sites parquet S3 snapshots.

Runs the heavy read of each ``ParquetRepository``'s primary dataset (see
:mod:`data_access_service.sites.sites_repository`) in a disposable Batch
container instead of the always-on API process, and writes the result back to
S3 as the snapshot the API process reloads. See
``data_access_service/sites/technical.md`` for the full design.
"""

from data_access_service import Config, init_log
from data_access_service.core.duckdbclient import SitesDuckDBClient
from data_access_service.sites.sites_repository import (
    ParquetRepository,
    build_repositories,
)

config = Config.get_config()
logger = init_log(config)


def refresh_sites_parquet_snapshots() -> None:
    """Refresh every registered sites repository's S3 snapshot.

    Always reloads from the primary dataset — the primary source updates
    often enough that a freshness pre-check rarely skips anything, so it's not
    worth the complexity. Builds its own :class:`SitesDuckDBClient` (this
    runs in its own disposable process, so unlike the old in-process scheduler
    there is no long-lived connection or S3 credentials to keep refreshed
    across runs).
    """
    session = SitesDuckDBClient()
    try:
        for name, repo in build_repositories(session).items():
            try:
                _refresh_one(name, repo)
            except Exception:
                logger.exception("Error refreshing repository '%s'", name)
    finally:
        session.close()


def _refresh_one(name: str, repo: ParquetRepository) -> None:
    """Reload one repository's table from its primary dataset and write the snapshot.

    Raises on failure — the caller logs and moves on to the next repository so
    one dataset's failure doesn't block the others.
    """
    logger.info("Refreshing repository '%s'...", name)
    repo.load()
    repo.write_snapshot()
    logger.info("Repository '%s' snapshot refreshed", name)
