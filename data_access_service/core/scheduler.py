import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from data_access_service import API, Config
from data_access_service.config.config import EnvType
from data_access_service.sites.sites_repository import ParquetRepository
from data_access_service.core.tiler_routes.startup import (
    RefreshInProgressError,
    refresh_tiler,
)
from data_access_service.utils.memory_utils import log_memory_usage

logger = logging.getLogger(__name__)


def _format_exception(exc: BaseException) -> str:
    """The exception message as a string.

    DuckDB errors containing non-UTF-8 bytes surface as UnicodeDecodeError,
    which hides the real message, so decode the raw bytes leniently instead.
    """
    if isinstance(exc, UnicodeDecodeError):
        recovered = exc.object.decode(exc.encoding, errors="replace")
        return f"{recovered} [recovered from non-UTF-8 DuckDB message; decode error: {exc}]"
    return str(exc)


class TaskScheduler:
    """Runs the recurring background jobs.

    1. Sites reload: reload each repository's table if its S3 snapshot changed.
    2. Tiler refresh: re-read store metadata and root_metadata.json.
    """

    def __init__(self, api: API, sites_repositories: dict[str, ParquetRepository]):
        self.api = api
        self.sites_repositories = sites_repositories
        self.scheduler = AsyncIOScheduler()

    def _reload_repository(self, name: str, repo: ParquetRepository):
        """Reload one repository if its snapshot changed.

        The snapshot S3 secret is recreated first so the credentials don't expire.
        """
        repo._configure_snapshot_bucket_s3()
        log_memory_usage(logger, f"before reload check '{name}'")
        try:
            if repo.reload_if_changed():
                logger.info("Repository '%s' reloaded from snapshot", name)
            else:
                logger.info("Repository '%s' snapshot unchanged; skipped", name)
        except Exception as e:
            logger.error(
                f"Error reloading repository '{name}': {_format_exception(e)}",
                exc_info=True,
            )
        log_memory_usage(logger, f"after reload check '{name}'")

    def _store_refresh_task(self):
        """Refresh the tiler's store metadata and product catalogue."""
        if not Config.is_profile_in(
            EnvType.EDGE,
            EnvType.STAGING,
            EnvType.PRODUCTION,
            EnvType.DEV,
            EnvType.TESTING,
        ):
            logger.info(
                "Skipping store refresh task on '%s' profile", Config.resolve_profile()
            )
            return
        logger.info("Store refresh task is running...")
        log_memory_usage(logger, "store refresh task start")
        try:
            refresh_tiler()
        except RefreshInProgressError:
            logger.info("Tiler refresh already running; skipped")
        except Exception:
            logger.exception("Store refresh task failed")
        log_memory_usage(logger, "store refresh task end")
        logger.info("Store refresh task completed")

    def _reload_task(self):
        """Reload every repository whose snapshot changed."""
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
        log_memory_usage(logger, "reload task start")
        for name, repo in self.sites_repositories.items():
            self._reload_repository(name, repo)
        log_memory_usage(logger, "reload task end")
        logger.info("Reload task completed")

    def _start(self):
        """Add the recurring jobs and start the scheduler."""
        self.scheduler.add_job(
            self._reload_task,
            trigger=CronTrigger(
                hour=f"*/{Config.get_config().get_sites_reload_interval_hours()}",
                minute="0",
            ),
            id="reload_task",
            name="Repository snapshot reload task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
        )

        self.scheduler.add_job(
            self._store_refresh_task,
            trigger=CronTrigger(
                hour=f"*/{Config.get_config().get_tiler_api_config().store_refresh_interval_hours}",
                minute="0",
            ),
            id="store_refresh_task",
            name="Tiler store refresh task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
        )

        logger.info("Starting task scheduler...")
        self.scheduler.start()
        logger.info("Task scheduler started successfully")

    async def start_with_initial_run(self):
        """Run the reload task once, then start the scheduler."""
        await self.api.wait_until_ready()

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            # Blocking S3 I/O, so keep it off the event loop.
            logger.info("Running reload task on startup...")
            await loop.run_in_executor(executor, self._reload_task)
        self._start()

    def shutdown(self):
        """Stop the scheduler, waiting for running jobs to finish."""
        logger.info("Shutting down task scheduler...")
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        logger.info("Task scheduler shut down successfully")
