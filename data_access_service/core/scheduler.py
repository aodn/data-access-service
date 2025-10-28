import logging
import duckdb
import asyncio
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aodn_cloud_optimised import DataQuery

logger = logging.getLogger(__name__)


class TaskScheduler:
    """Manages scheduled tasks for the application."""

    def __init__(self):
        self.memconn = duckdb.connect(":memory:cloud_optimized")
        self.scheduler = AsyncIOScheduler()
        self._instance = DataQuery.GetAodn()

    def hourly_task(self):
        """
        Example task that runs every hour.
        Replace this with your actual task logic.
        """
        logger.info("Hourly task is running...")
        dataset = f"s3://{self._instance.bucket_name}/wave_buoy_realtime_nonqc.parquet"
        self.memconn.execute(
            f"""
            CREATE OR REPLACE TABLE wave_buoy_realtime_nonqc AS
            SELECT *
            FROM read_parquet('{dataset}/**/*.parquet', hive_partitioning=true)"""
        )

        logger.info("Hourly task completed")

    def start(self):
        """Start the scheduler and add jobs."""
        self.scheduler.add_job(
            self.hourly_task,
            trigger=CronTrigger(hour="*", minute="0"),  # Every hour at :00
            id="hourly_task",
            name="Hourly scheduled task",
            replace_existing=True,
        )

        logger.info("Starting task scheduler...")
        self.scheduler.start()
        logger.info("Task scheduler started successfully")

    async def start_with_initial_run(self):
        """Start the scheduler and run the hourly task immediately."""
        # Use ThreadPoolExecutor to run blocking calls in a separate thread
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # Schedule the blocking calls in a thread
            await loop.run_in_executor(executor, lambda: self.hourly_task())
        self.start()
        logger.info("Running hourly task on startup...")

    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        logger.info("Shutting down task scheduler...")
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        logger.info("Task scheduler shut down successfully")
