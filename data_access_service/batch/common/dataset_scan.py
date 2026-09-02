"""Shared plumbing for batch jobs that scan one cloud-optimised parquet dataset."""

from typing import Optional

from data_access_service import Config, init_log
from data_access_service.batch.common.dataset_location import (
    DatasetLocation,
    resolve_dataset_location,
)
from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)
from data_access_service.core.duckdbclient import PmTileDuckDBClient
from data_access_service.models.duckdb_types import DuckDBTuningConfig
from data_access_service.utils.memory_utils import log_memory_usage


class DatasetScanBase:
    """One dataset, one work directory, one DuckDB session."""

    def __init__(
        self,
        uuid: str,
        dataset_name: str,
        work_dir: str,
        api: BaseAPI,
        duckdb_tuning: Optional[DuckDBTuningConfig] = None,
        location: Optional[DatasetLocation] = None,
    ):
        """``duckdb_tuning`` defaults to the pmtiles job's DuckDB settings.

        Subclasses whose memory profile differs (the estimation index has no
        tippecanoe to leave room for) pass their own.

        ``location`` defaults to looking the dataset up: not every dataset is
        in the AODN bucket, and one hosted elsewhere needs its own credentials
        before DuckDB can read a single file.
        """
        self.work_dir = work_dir
        self.uuid = uuid
        self.dataset_name = dataset_name
        self.api = api
        self.config = Config.get_config()
        self.logger = init_log(self.config)
        self.location = (
            location if location is not None else resolve_dataset_location(dataset_name)
        )
        self.pm_client = PmTileDuckDBClient(tuning=duckdb_tuning)
        if self.location.is_external:
            self.logger.info(
                "Dataset %s is hosted outside AODN; adding an S3 secret for %s",
                self.dataset_name,
                self.location.bucket,
            )
            self.pm_client.create_s3_secret_with_keys(
                bucket=self.location.bucket,
                access_key=self.location.access_key,
                secret_access_key=self.location.secret_access_key,
                endpoint=self.location.endpoint,
                use_ssl=self.location.use_ssl,
            )

    # The s3 uri of the source parquet. It is not http URL of s3 objects.
    def get_s3_uri(self):
        return self.location.parquet_glob(self.dataset_name)

    def get_source_sql(self) -> str:
        """The source dataset as a table expression, for use in a FROM clause.

        Built by the client so every job reads the dataset the same way (Hive
        keys exposed, union_by_name only when the dataset needs it).
        """
        return self.pm_client.parquet_source_sql(self.get_s3_uri())

    def get_lat_col_name(self) -> str:
        lat_mapped = self.api.map_column_names(
            uuid=self.uuid, key=self.dataset_name, columns=[STR_LATITUDE_UPPER_CASE]
        )
        if not lat_mapped:
            raise ValueError(
                f"Could not find latitude column for dataset {self.dataset_name}"
            )
        return lat_mapped[0]

    def get_lon_col_name(self) -> str:
        lon_mapped = self.api.map_column_names(
            uuid=self.uuid, key=self.dataset_name, columns=[STR_LONGITUDE_UPPER_CASE]
        )
        if not lon_mapped:
            raise ValueError(
                f"Could not find longitude column for dataset {self.dataset_name}"
            )
        return lon_mapped[0]

    def get_time_col_name(self) -> Optional[str]:
        """Mapped TIME column name, or None when the dataset has no time field.

        Callers that support timeless datasets (synthetic single period) must
        handle None; lat/lon remain required.
        """
        time_mapped = self.api.map_column_names(
            uuid=self.uuid, key=self.dataset_name, columns=[STR_TIME_UPPER_CASE]
        )
        if not time_mapped:
            return None
        return time_mapped[0]

    def _release_duckdb(self, checkpoint: str) -> None:
        """Close the client cursor and tear down the process-global DuckDB connection.

        Idempotent: safe to call more than once (e.g. before tippecanoe and again
        in ``process``'s ``finally``). After this, a new :class:`PmTileDuckDBClient`
        (next dataset) rebuilds the connection lazily via ``get_instance``.

        Step logs are intentional breadcrumbs: a native SIGSEGV during teardown
        cannot be caught in Python, so the last line shows which close step ran.
        """
        self.logger.info("Releasing DuckDB (%s): closing client cursor", checkpoint)
        try:
            self.pm_client.close()
        except Exception:
            self.logger.exception(
                "PmTileDuckDBClient.close() failed (%s); continuing to shutdown",
                checkpoint,
            )

        self.logger.info(
            "Releasing DuckDB (%s): shutting down process-global connection",
            checkpoint,
        )
        try:
            PmTileDuckDBClient.shutdown()
        except Exception:
            self.logger.exception(
                "PmTileDuckDBClient.shutdown() failed (%s); continuing",
                checkpoint,
            )

        self.logger.info("DuckDB released (%s)", checkpoint)
        log_memory_usage(self.logger, f"after duckdb shutdown ({checkpoint})")
