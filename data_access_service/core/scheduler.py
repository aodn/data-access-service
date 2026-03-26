import logging
import boto3
import duckdb
import asyncio
from concurrent.futures import ThreadPoolExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aodn_cloud_optimised import DataQuery
from data_access_service.config.config import Config

logger = logging.getLogger(__name__)


class TaskScheduler:
    """Manages scheduled tasks for the application."""

    WAVE_BUOY_TABLE_NAME = "wave_buoy_realtime_nonqc"

    def __init__(self):
        self.memconn = duckdb.connect(":memory:cloud_optimized")
        self.scheduler = AsyncIOScheduler()
        self._instance = DataQuery.GetAodn()
        self.wave_buoy_backup_bucket = (
            Config.get_config().get_wave_buoy_backup_bucket_name()
        )
        self.wave_buoy_backup_s3_path = f"s3://{self.wave_buoy_backup_bucket}/imoslive/BUOY/{self.WAVE_BUOY_TABLE_NAME}.parquet"
        self._configure_duckdb_s3()

    def _configure_duckdb_s3(self):
        # The source bucket (aodn-cloud-optimised) is public, so DuckDB can read it
        # without credentials. The backup bucket is private, so DuckDB needs explicit
        # credentials for both read and write.
        #
        # DuckDB's own credential chain does not reliably handle SSO locally or ECS
        # task roles on deployed environments. Instead, we resolve credentials via
        # boto3 — which handles all providers correctly (SSO locally, task role on ECS)
        # — and pass them to DuckDB as a named secret.
        #
        # ECS task role credentials expire after ~6 hours. This method is called at
        # startup and before every hourly_task run to keep the secret current.
        session = boto3.Session()
        creds = session.get_credentials().get_frozen_credentials()
        region = session.region_name or "ap-southeast-2"
        self.memconn.execute(
            f"""
            CREATE OR REPLACE SECRET wave_buoy_s3 (
                TYPE S3,
                KEY_ID '{creds.access_key}',
                SECRET '{creds.secret_key}',
                SESSION_TOKEN '{creds.token or ""}',
                REGION '{region}',
                SCOPE 's3://{self.wave_buoy_backup_bucket}'
            )
        """
        )

    def hourly_task(self):
        """
        Refreshes the wave buoy data table without blocking reads.
        Uses a temporary table strategy to avoid locking the main table during refresh.
        """
        logger.info("Hourly task is running...")
        # Refresh S3 credentials before each run so the DuckDB secret never expires
        # (ECS task role credentials are valid for ~6 hours and boto3 always returns
        # fresh ones, so calling this every 2 hours keeps the secret current)
        self._configure_duckdb_s3()
        temp_table_name = f"{self.WAVE_BUOY_TABLE_NAME}_temp"
        target_table_name = self.WAVE_BUOY_TABLE_NAME

        try:
            dataset = (
                f"s3://{self._instance.bucket_name}/wave_buoy_realtime_nonqc.parquet"
            )

            # Step 1: Create temp table with new data (non-blocking for readers)
            logger.info(f"Creating temporary table '{temp_table_name}'...")
            self.memconn.execute(
                f"""
                CREATE OR REPLACE TABLE {temp_table_name} AS
                SELECT LATITUDE, LONGITUDE, TIME, SSWMD, WPFM, WPMH, WHTH, WSSH, site_name
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

            # Keep backup fresh
            self.memconn.execute(
                f"COPY {target_table_name} TO '{self.wave_buoy_backup_s3_path}' (FORMAT PARQUET)"
            )
            logger.info("Backup written to S3 successfully")
            logger.info("Hourly task completed successfully")
        except Exception as e:
            logger.error(f"Error in hourly task: {e}", exc_info=True)
            # Clean up temp table if it exists
            try:
                self.memconn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                logger.info(f"Cleaned up temporary table '{temp_table_name}'")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temp table: {cleanup_error}")

            # Fallback: restore from backup so the endpoint stays healthy
            try:
                self.memconn.execute(
                    f"""
                    CREATE OR REPLACE TABLE {target_table_name} AS
                    SELECT * FROM read_parquet('{self.wave_buoy_backup_s3_path}')
                    """
                )
                logger.info("Loaded from S3 backup successfully")
            except Exception as backup_error:
                logger.error(
                    f"Failed to load from S3 backup: {backup_error}", exc_info=True
                )

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
        # Pre-load from backup so the endpoint is available while the full S3 refresh runs
        try:
            self.memconn.execute(
                f"""
                CREATE OR REPLACE TABLE {self.WAVE_BUOY_TABLE_NAME} AS
                SELECT * FROM read_parquet('{self.wave_buoy_backup_s3_path}')
                """
            )
            logger.info("Pre-loaded wave buoy data from S3 backup")
        except Exception as e:
            logger.warning(f"No S3 backup found, will rely on initial S3 fetch: {e}")

        # Full refresh from main S3
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, lambda: self.hourly_task())
        self.start()
        logger.info("Running hourly task on startup...")

    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        logger.info("Shutting down task scheduler...")
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
        logger.info("Task scheduler shut down successfully")
