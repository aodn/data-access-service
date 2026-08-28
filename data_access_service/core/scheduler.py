import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from data_access_service import API
from data_access_service import Config
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
    """Refreshes every registered :class:`ParquetRepository` on a schedule.

    Each repository owns its own dataset locations and the SQL to (re)load it
    (see :mod:`data_access_service.sites.duckdb_repository`); this scheduler just
    drives the loads. The repositories share the single ``ParquetDuckDBClient`` built in
    :mod:`data_access_service.server`, so every read endpoint sees the refreshed
    tables.
    """

    def __init__(self, api: API, repositories: dict[str, ParquetRepository]):
        self.api = api
        self.repositories = repositories
        self.scheduler = AsyncIOScheduler()

    def _refresh_repository(
        self, name: str, repo: ParquetRepository, *, incremental: bool = False
    ):
        # Refresh the S3 secrets before each run so they never expire. ECS task
        # role credentials are valid for ~6 hours and boto3 always returns fresh
        # ones, so re-creating the secrets every refresh keeps them current.
        repo._configure_s3()
        repo._configure_backup_s3()
        verb = "Incrementally refreshing" if incremental else "Refreshing"
        try:
            logger.info(f"{verb} repository '{name}' from primary dataset...")
            if incremental:
                repo.load_incremental()
            else:
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
        """Full reload of every registered repository (the weekly scheduled job).

        Runs on a slower cadence than :meth:`_incremental_refresh_task` because
        it is the only one that can catch a retroactive correction to data
        older than the incremental job's lookback window — see
        ``ParquetRepository.load_incremental``.
        """
        if not Config.is_profile_in(
            EnvType.EDGE,
            EnvType.STAGING,
            EnvType.PRODUCTION,
            EnvType.DEV,
            EnvType.TESTING,
        ):
            logger.info(
                "Skipping refresh task on '%s' profile", Config.resolve_profile()
            )
            return
        logger.info("Refresh task is running...")
        for name, repo in self.repositories.items():
            self._refresh_repository(name, repo, incremental=False)
        logger.info("Refresh task completed")

    def _incremental_refresh_task(self):
        """Incremental reload of every registered repository (the frequent job).

        Only re-reads the most recent Hive partitions instead of the whole
        dataset — see ``ParquetRepository.load_incremental``.
        """
        if not Config.is_profile_in(
            EnvType.EDGE,
            EnvType.STAGING,
            EnvType.PRODUCTION,
            EnvType.DEV,
            EnvType.TESTING,
        ):
            logger.info(
                "Skipping incremental refresh task on '%s' profile",
                Config.resolve_profile(),
            )
            return
        logger.info("Incremental refresh task is running...")
        for name, repo in self.repositories.items():
            self._refresh_repository(name, repo, incremental=True)
        logger.info("Incremental refresh task completed")

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

    @staticmethod
    def _backup_is_fresh(repo: ParquetRepository) -> bool:
        """Whether the preloaded backup is recent enough to skip a full reload."""
        if not repo.is_loaded():
            return False
        latest = repo.latest_time()
        if latest is None:
            return False
        cutoff = repo._incremental_cutoff(repo.incremental_lookback_days)
        return latest >= cutoff.to_pydatetime()

    def _initial_refresh_task(self):
        """Startup refresh: full reload only where the preloaded backup needs it."""
        if not Config.is_profile_in(
            EnvType.EDGE,
            EnvType.STAGING,
            EnvType.PRODUCTION,
            EnvType.DEV,
            EnvType.TESTING,
        ):
            logger.info(
                "Skipping initial refresh task on '%s' profile",
                Config.resolve_profile(),
            )
            return
        logger.info("Initial refresh task is running...")
        for name, repo in self.repositories.items():
            incremental = self._backup_is_fresh(repo)
            verb = "incremental catch-up" if incremental else "full reload"
            logger.info(
                f"Repository '{name}': backup {'is' if incremental else 'is not'} "
                f"fresh enough, running {verb}"
            )
            self._refresh_repository(name, repo, incremental=incremental)
        logger.info("Initial refresh task completed")

    def _start(self):
        """Start the scheduler and add the recurring refresh jobs.

        Full reload once a week (an odd hour, so it never coincides with the
        every-2-hours incremental job below) as a safety net for retroactive
        corrections; incremental reload the rest of the time for the low
        memory/CPU cost.
        """
        self.scheduler.add_job(
            self._refresh_task,
            trigger=CronTrigger(
                day_of_week="sun", hour="3", minute="0"
            ),  # Weekly, Sunday 03:00 UTC
            id="refresh_task",
            name="Repository data full refresh task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
        )
        self.scheduler.add_job(
            self._incremental_refresh_task,
            trigger=CronTrigger(hour="*/2", minute="0"),  # Every 2 hours at :00
            id="incremental_refresh_task",
            name="Repository data incremental refresh task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
        )

        logger.info("Starting task scheduler...")
        self.scheduler.start()
        logger.info("Task scheduler started successfully")

    async def start_with_initial_run(self):
        # API init is memory intensive, so do not refresh until the init is done
        await self.api.wait_until_ready()

        """Start the scheduler and run the refresh task immediately."""
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            # Pre-load from backup so the endpoints are available while the initial S3
            # refresh runs. Both are blocking S3 reads — run in an executor to
            # avoid blocking the event loop.
            logger.info("Running refresh task on startup...")
            await loop.run_in_executor(executor, self._preload_from_backup)
            await loop.run_in_executor(executor, self._initial_refresh_task)
        self._start()

    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        logger.info("Shutting down task scheduler...")
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        logger.info("Task scheduler shut down successfully")
