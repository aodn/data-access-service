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
        Refreshes the wave buoy data table without blocking reads.
        Uses a temporary table strategy to avoid locking the main table during refresh.
        """
        logger.info("Hourly task is running...")
        temp_table_name = "wave_buoy_realtime_nonqc_temp"
        target_table_name = "wave_buoy_realtime_nonqc"

        try:
            dataset = (
                f"s3://{self._instance.bucket_name}/wave_buoy_realtime_nonqc.parquet"
            )

            # Step 1: Create temp table with new data (non-blocking for readers)
            logger.info(f"Creating temporary table '{temp_table_name}'...")
            self.memconn.execute(
                f"""
                CREATE OR REPLACE TABLE {temp_table_name} AS
                SELECT *
                FROM read_parquet('{dataset}/**/*.parquet', hive_partitioning=true)"""
            )
            logger.info(f"Temporary table '{temp_table_name}' created successfully")

            # Step 2: Drop old table if it exists
            logger.info(f"Dropping old table '{target_table_name}'...")
            self.memconn.execute(
                f"""
                BEGIN TRANSACTION;
                DROP TABLE IF EXISTS {target_table_name};
                ALTER TABLE {temp_table_name} RENAME TO {target_table_name};
                COMMIT;
            """
            )

            logger.info("Hourly task completed successfully")
        except Exception as e:
            logger.error(f"Error in hourly task: {e}", exc_info=True)
            # Clean up temp table if it exists
            try:
                self.memconn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                logger.info(f"Cleaned up temporary table '{temp_table_name}'")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temp table: {cleanup_error}")

    def start(self):
        """Start the scheduler and add jobs."""
        self.scheduler.add_job(
            self.hourly_task,
            trigger=CronTrigger(hour="*/2", minute="0"),  # Every 2 hours at :00
            id="hourly_task",
            name="Hourly scheduled task",
            replace_existing=True,
            coalesce=True,
            misfire_grace_time=None,
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
