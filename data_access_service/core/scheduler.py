import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from data_access_service import API, Config
from data_access_service.config.config import EnvType
from data_access_service.sites.sites_repository import ParquetRepository
from data_access_service.tiler.services.store.registry import refresh_stores
from data_access_service.utils.memory_utils import log_memory_usage

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
    """Runs the app's recurring background jobs on a single APScheduler instance.

    Two independent jobs:

    1. Keeps every registered :class:`ParquetRepository`'s table in sync with
       its S3 snapshot. The heavy read of each dataset's primary source runs
       in a separate AWS Batch job (see
       ``data_access_service/batch/sites_parquet/refresher.py``), which
       writes the result to S3 as a flat snapshot file — see
       ``data_access_service/sites/technical.md`` for the full design. This
       scheduler only ever does the cheap side: on a recurring schedule, a
       single S3 HEAD per repository to check its snapshot's ETag, and —
       only if it changed — a lightweight reload. The repositories share the
       single ``SitesDuckDBClient`` built in :mod:`data_access_service.server`,
       so every read endpoint sees the reloaded tables.
    2. Re-opens every tiler store already published in
       ``StoreRegistry._stores`` (see
       ``data_access_service/tiler/services/store/registry.py``) on a fixed
       interval, bounding data staleness regardless of how often a given
       store is requested — see ``tiler/technical.md`` for the design this
       replaced (request-triggered TTL refresh).
    """

    def __init__(self, api: API, sites_repositories: dict[str, ParquetRepository]):
        self.api = api
        self.sites_repositories = sites_repositories
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
        """Re-open every currently-valid tiler store (the scheduled job).

        Sequential (one store at a time) by design, so this never opens more
        than one Zarr store's metadata at once regardless of how many stores
        are registered — the peak-memory/CPU stampede this replaced came from
        several request-triggered refreshes overlapping.
        """
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
            refresh_stores()
        except Exception:
            logger.exception("Store refresh task failed")
        log_memory_usage(logger, "store refresh task end")
        logger.info("Store refresh task completed")

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
        log_memory_usage(logger, "reload task start")
        for name, repo in self.sites_repositories.items():
            self._reload_repository(name, repo)
        log_memory_usage(logger, "reload task end")
        logger.info("Reload task completed")

    def _start(self):
        """Start the scheduler and add the recurring jobs."""
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
                hour=f"*/{Config.get_config().get_tiler_config().store_refresh_interval_hours}",
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
        """Start the scheduler and run the reload task immediately."""
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
