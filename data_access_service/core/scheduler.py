import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from data_access_service import API
from data_access_service.repositories.duckdb_repository import ParquetRepository

logger = logging.getLogger(__name__)


def _format_exception(exc: BaseException) -> str:
    """Render an exception message, recovering DuckDB errors whose bytes are not valid UTF-8.

    DuckDB's Python binding decodes its C++ error messages as UTF-8. When a message
    contains a non-UTF-8 byte (e.g. raw bytes from a corrupt or non-Parquet S3 object),
    the decode itself raises UnicodeDecodeError, masking the real error. The raw message
    bytes are preserved on the exception's ``object`` attribute, so we recover them here
    with ``errors="replace"`` rather than letting the cryptic decode error surface.
    """
    if isinstance(exc, UnicodeDecodeError):
        recovered = exc.object.decode(exc.encoding, errors="replace")
        return f"{recovered} [recovered from non-UTF-8 DuckDB message; decode error: {exc}]"
    return str(exc)


class TaskScheduler:
    """Refreshes every registered :class:`ParquetRepository` on a schedule.

    Each repository owns its own dataset locations and the SQL to (re)load it
    (see :mod:`data_access_service.repositories.duckdb_repository`); this scheduler just
    drives the loads. The repositories share the single ``DuckDBSession`` built in
    :mod:`data_access_service.server`, so every read endpoint sees the refreshed
    tables.
    """

    def __init__(self, api: API, repositories: dict[str, ParquetRepository]):
        self.api = api
        self.repositories = repositories
        self.scheduler = AsyncIOScheduler()

    async def _wait_until_api_ready(self, timeout: float = 300):
        """Wait until main API metadata initialization has finished."""
        waited = 0
        while not self.api.get_api_status():
            if waited >= timeout:
                logger.warning("Timed out waiting for API to become ready")
                break
            await asyncio.sleep(0.5)
            waited += 0.5
        logger.info(
            f"API ready status = {self.api.get_api_status()} (waited {waited}s)"
        )

    def _refresh_repository(self, name: str, repo: ParquetRepository):
        """Reload one repository from its primary dataset, then refresh its backup.

        ``CREATE OR REPLACE TABLE`` (inside ``repo.load``) is atomic, so readers
        keep seeing the previous table until the new one is committed; a failed
        read rolls back and leaves the existing table intact. The backup write is
        best-effort — a failure there does not affect the freshly loaded table.
        """
        # Refresh the S3 secrets before each run so they never expire. ECS task
        # role credentials are valid for ~6 hours and boto3 always returns fresh
        # ones, so re-creating the secrets every refresh keeps them current.
        repo._configure_s3()
        repo._configure_backup_s3()
        try:
            logger.info(f"Refreshing repository '{name}' from primary dataset...")
            repo.load()
            logger.info(f"Repository '{name}' refreshed successfully")
        except Exception as e:
            logger.error(
                f"Error refreshing repository '{name}': {_format_exception(e)}",
                exc_info=True,
            )
            return

        try:
            repo.write_backup()
            logger.info(f"Backup written for repository '{name}'")
        except Exception as e:
            logger.warning(
                f"Failed to write backup for repository '{name}': {_format_exception(e)}"
            )

    def _refresh_task(self):
        """Refresh every registered repository (the scheduled job)."""
        logger.info("Refresh task is running...")
        for name, repo in self.repositories.items():
            self._refresh_repository(name, repo)
        logger.info("Refresh task completed")

    def _preload_from_backup(self):
        """Seed every repository from its S3 backup so endpoints work during the refresh.

        Best-effort: on a first-ever run no backup exists yet, which is logged and
        ignored — the subsequent primary refresh will populate the table.
        """
        for name, repo in self.repositories.items():
            try:
                repo.load_backup()
                logger.info(f"Pre-loaded repository '{name}' from S3 backup")
            except Exception as e:
                logger.warning(
                    f"No S3 backup to pre-load for repository '{name}', "
                    f"will rely on initial S3 refresh: {_format_exception(e)}"
                )

    def _start(self):
        """Start the scheduler and add the recurring refresh job."""
        self.scheduler.add_job(
            self._refresh_task,
            trigger=CronTrigger(hour="*/2", minute="0"),  # Every 2 hours at :00
            id="refresh_task",
            name="Repository data refresh task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
        )

        logger.info("Starting task scheduler...")
        self.scheduler.start()
        logger.info("Task scheduler started successfully")

    async def start_with_initial_run(self):
        # API init is memory intensive, so do not refresh until the init is done
        await self._wait_until_api_ready()

        """Start the scheduler and run the refresh task immediately."""
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            # Pre-load from backup so the endpoints are available while the full S3
            # refresh runs. Both are blocking S3 reads — run in an executor to
            # avoid blocking the event loop.
            logger.info("Running refresh task on startup...")
            await loop.run_in_executor(executor, self._preload_from_backup)
            await loop.run_in_executor(executor, self._refresh_task)
        self._start()

    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        logger.info("Shutting down task scheduler...")
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        logger.info("Task scheduler shut down successfully")
