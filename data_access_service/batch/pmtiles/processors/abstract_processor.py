import subprocess
import time
import os

from abc import ABC, abstractmethod
from typing import List, Optional, Sequence
from aodn_cloud_optimised.lib.DataQuery import BUCKET_OPTIMISED_DEFAULT
from data_access_service import Config, init_log
from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)
from data_access_service.core.duckdbclient import PmTileDuckDBClient
from data_access_service.models.pmtiles_types import (
    PmtilesGenerationConfig,
    PmtilesLayerSpec,
)
from data_access_service.utils.date_time_utils import time_it
from data_access_service.utils.memory_utils import log_memory_usage


class AbstractProcessor(ABC):

    def __init__(self, uuid: str, dataset_name: str, work_dir: str, api: BaseAPI):
        self.work_dir = work_dir
        self.uuid = uuid
        self.dataset_name = dataset_name
        self.api = api
        self.config = Config.get_config()
        self.logger = init_log(self.config)
        self.pm_client = PmTileDuckDBClient()

        # These dirs are relative dir names. If they start with "/", they will be absolute dir names and will cause further problems.
        self.pmtiles_config: PmtilesGenerationConfig = self.config.get_pmtiles_config()
        if self.pmtiles_config.staged_parquet_dir.startswith(
            "/"
        ) or self.pmtiles_config.duckdb_temp_dir.startswith("/"):
            raise ValueError("dir must be a relative path")

    def process(self) -> str:

        try:
            self.build_staging_parquet()
            self.logger.info(
                f"Finished building staging parquet for dataset {self.dataset_name} with UUID {self.uuid}"
            )

            # Third step, generate GeoJSONSeq files from staging parquet. GeoJSONSeq files are data input of PMTiles
            geojsonseq_paths = self.generate_geojsonseq_files()
            self.logger.info(
                f"Finished generating GeoJSONSeq files for dataset {self.dataset_name} with UUID {self.uuid}: {geojsonseq_paths}"
            )

            # The staged parquet is only read while generating GeoJSONSeq files;
            # remove it now so it does not sit on disk alongside the geojsonseq
            # files and tippecanoe's temp files during the peak-disk step below.
            self._remove_staged_parquet()

            # Fourth step, use all GeoJSONSeq files to generate PMTiles file.
            self.logger.info(
                f"Generating pmtiles file for dataset {self.dataset_name} with UUID {self.uuid}..."
            )
            pmtile_path = self.generate_pmtiles_file(geojsonseq_paths=geojsonseq_paths)
            self.logger.info(
                f"Finished generating pmtiles file for dataset {self.dataset_name} with UUID {self.uuid}. Output path: {pmtile_path}"
            )
            self._remove_geojsonseq_files(geojsonseq_paths)
            return pmtile_path

        finally:
            self.pm_client.close()
            # Tear down the process-global connection too, so its buffer pool
            # and caches are released between datasets instead of ratcheting
            # up the process RSS over a multi-dataset batch run.
            PmTileDuckDBClient.shutdown()
            self.logger.debug("DuckDB connection closed")
            log_memory_usage(self.logger, "after duckdb shutdown")

    # The s3 uri of the source parquet. It is not http URL of s3 objects.
    def get_s3_uri(self):
        return f"s3://{BUCKET_OPTIMISED_DEFAULT}/{self.dataset_name}/**/*.parquet"

    def get_staged_path(self) -> str:
        return os.path.join(
            self.work_dir,
            self.pmtiles_config.staged_parquet_dir,
            "staged_high_res.parquet",
        )

    def get_output_pmtiles_path(self) -> str:
        return os.path.join(
            self.work_dir,
            self.pmtiles_config.output_pmtiles_dir,
            f"{self.dataset_name}.pmtiles",
        )

    def get_geojsonseq_dir(self) -> str:
        return os.path.join(self.work_dir, self.pmtiles_config.geojsonseq_dir)

    def _remove_staged_parquet(self) -> None:
        staged_path = self.get_staged_path()
        if os.path.exists(staged_path):
            os.remove(staged_path)
            self.logger.info(
                f"Removed staged parquet {staged_path!r} to free disk before tippecanoe"
            )

    def _remove_geojsonseq_files(self, geojsonseq_paths: List[str]) -> None:
        removed = 0
        for path in geojsonseq_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    removed += 1
            except Exception as e:
                self.logger.warning(f"Failed to remove {path}: {e}")
        self.logger.info(
            f"Removed {removed}/{len(geojsonseq_paths)} GeoJSONSeq file(s)"
        )

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

    def get_time_col_name(self) -> str:
        time_mapped = self.api.map_column_names(
            uuid=self.uuid, key=self.dataset_name, columns=[STR_TIME_UPPER_CASE]
        )
        if not time_mapped:
            raise ValueError(
                f"Could not find timestamp column for dataset {self.dataset_name}"
            )
        return time_mapped[0]

    @time_it
    def generate_pmtiles_file(
        self,
        geojsonseq_paths: List[str],
        extra_args: Optional[List[str]] = None,
        max_threads: int = 1,
    ) -> str:
        output_pmtiles_path = self.get_output_pmtiles_path()
        os.makedirs(os.path.dirname(os.path.abspath(output_pmtiles_path)))

        # The memory usage increase linearly with the number of threads,
        # By default, Tippecanoe spins up as many parallel worker threads as you have CPU cores. Each concurrent thread
        # processes geographic tiles independently, which multiplies the amount of geometry held in your RAM
        # simultaneously.
        cmd = [
            "tippecanoe",
            f"--output={output_pmtiles_path}",
            "--force",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--read-parallel",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            *geojsonseq_paths,
        ]

        if extra_args:
            cmd.extend(extra_args)

        env = {**os.environ, "TIPPECANOE_MAX_THREADS": str(max_threads)}

        self.logger.info(
            f"Generating {output_pmtiles_path!r} from {len(geojsonseq_paths)} GeoJSONSeq file(s) using Tippecanoe"
        )
        self.logger.debug(
            f"Full Tippecanoe command: TIPPECANOE_MAX_THREADS={max_threads} {' '.join(cmd)}"
        )
        # Only covers this Python process
        log_memory_usage(self.logger, "before tippecanoe")
        subprocess.run(cmd, check=True, env=env)
        log_memory_usage(self.logger, "after tippecanoe")
        self.logger.info(f"Finished generating {output_pmtiles_path!r}")
        return output_pmtiles_path

    # The layers are the layer config of the PMTiles file. Different Visualization styles should have different layer configs.
    # PmtilesLayerSpec is the superclass of different style layer configs.
    @abstractmethod
    def get_layers(self) -> Sequence[PmtilesLayerSpec]:
        pass

    # The staging parquet is a local parquet which is cached, abstracted and simply pre-calculated parquet according to remote source parquet.
    # This step saves the IOs and speed up the processing.
    @abstractmethod
    def build_staging_parquet(self):
        pass

    # The GeoJSONSeq files are generated from staging parquet. Each GeoJSONSeq file corresponds to one layer in PMTiles.
    # All the GeoJSONSeq files are data source of the PMTiles.
    @abstractmethod
    def generate_geojsonseq_files(self) -> List[str]:
        pass
