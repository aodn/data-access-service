import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from data_access_service import API, Config
from data_access_service.config.config import EnvType
from data_access_service.sites.sites_repository import ParquetRepository

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
    """Keeps every registered :class:`ParquetRepository`'s table in sync with its S3 snapshot.

    The heavy read of each dataset's primary source runs in a separate AWS
    Batch job (see ``data_access_service/batch/sites_parquet/refresher.py``),
    which writes the result to S3 as a flat snapshot file — see
    ``data_access_service/sites/technical.md`` for the full design. This
    scheduler only ever does the cheap side: on a recurring schedule, a single
    S3 HEAD per repository to check its snapshot's ETag, and — only if it
    changed — a lightweight reload. The repositories share the single
    ``ParquetDuckDBClient`` built in :mod:`data_access_service.server`, so
    every read endpoint sees the reloaded tables.
    """

    def __init__(self, api: API, repositories: dict[str, ParquetRepository]):
        self.api = api
        self.repositories = repositories
        self.scheduler = AsyncIOScheduler()

    def _reload_repository(self, name: str, repo: ParquetRepository):
        """Reload one repository's table from its S3 snapshot if it changed.

        Only the snapshot-bucket S3 secret needs refreshing here — this process
        never reads the primary dataset, so it never needs the primary
        bucket's secret. ECS task role credentials are valid for ~6 hours and
        boto3 always returns fresh ones, so re-creating the secret every
        reload keeps it current.
        """
        repo._configure_snapshot_bucket_s3()
        try:
            if repo.reload_if_changed():
                logger.info("Repository '%s' reloaded from snapshot", name)
            else:
                logger.info("Repository '%s' snapshot unchanged; skipped", name)
        except Exception as e:
            # Not logger.exception(): _format_exception recovers non-UTF-8 DuckDB
            # error bytes that would otherwise mask the real error, and passing
            # the exception object to .exception() would repeat that same decode.
            logger.error(
                f"Error reloading repository '{name}': {_format_exception(e)}",
                exc_info=True,
            )

    def _reload_task(self):
        """Reload every registered repository whose snapshot changed (the scheduled job)."""
        if not Config.is_profile_in(
            EnvType.EDGE,
            EnvType.STAGING,
            EnvType.PRODUCTION,
            EnvType.DEV,
            EnvType.TESTING,
        ):
            logger.info(
                "Skipping reload task on '%s' profile", Config.resolve_profile()
            )
            return
        logger.info("Reload task is running...")
        for name, repo in self.repositories.items():
            self._reload_repository(name, repo)
        logger.info("Reload task completed")

    def _start(self):
        """Start the scheduler and add the recurring reload job."""
        self.scheduler.add_job(
            self._reload_task,
            trigger=CronTrigger(minute="0"),  # Every hour, on the hour
            id="reload_task",
            name="Repository snapshot reload task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
        )

        logger.info("Starting task scheduler...")
        self.scheduler.start()
        logger.info("Task scheduler started successfully")

    async def start_with_initial_run(self):
        """Start the scheduler and run the reload task immediately."""
        # API init is memory intensive, so do not reload until the init is done
        await self.api.wait_until_ready()

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            # Reload is cheap (one HEAD + a small-file read per repository) but
            # still blocking S3 I/O, so keep it off the event loop at startup.
            logger.info("Running reload task on startup...")
            await loop.run_in_executor(executor, self._reload_task)
        self._start()

    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        logger.info("Shutting down task scheduler...")
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        logger.info("Task scheduler shut down successfully")
