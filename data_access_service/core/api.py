from data_access_service import Config
import asyncio
import gzip
import math
import os
import gc

import duckdb
import json
import zlib


from concurrent.futures import ThreadPoolExecutor

import dask.dataframe as ddf
import pandas as pd
import logging
import xarray

from datetime import timedelta, timezone
from io import BytesIO
from typing import Optional, Dict, Any, List, Tuple, Hashable
from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource, ZarrDataSource
from aodn_cloud_optimised.lib.config import get_notebook_url
from bokeh.server.tornado import psutil
from xarray.core.utils import Frozen

from data_access_service.core.constants import (
    STR_LATITUDE_UPPER_CASE,
    STR_LATITUDE_LOWER_CASE,
    STR_LONGITUDE_UPPER_CASE,
    STR_LONGITUDE_LOWER_CASE,
    STR_TIME_UPPER_CASE,
    OUTPUT_FORMAT_COMPRESSION_RATIO,
    COMPRESSION_RATIO_GEOTIFF,
    GEOTIFF_ZIP_RATIO,
    GEOTIFF_INT_PIXEL_BYTES,
)
from data_access_service.models.subset_request import SUPPORTED_OUTPUT_FORMATS
from data_access_service.core.descriptor import Depth, Descriptor, Coordinate
from urllib.parse import unquote_plus


log = logging.getLogger(__name__)

HEALTH_JSON = "/tmp/status/health.json"


def gzip_compress(data):
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data.encode("utf-8"))
    return buf.getvalue()


class BaseAPI:
    def get_temporal_extent(
        self, uuid: str, key: str
    ) -> Tuple[pd.Timestamp | None, pd.Timestamp | None]:
        pass

    def get_mapped_meta_data(self, uuid: str | None) -> Dict[str, Descriptor]:
        pass

    def has_data(
        self, uuid: str, key: str, start_date: pd.Timestamp, end_date: pd.Timestamp
    ):
        pass

    def get_dataset(
        self,
        uuid: str,
        key: str,
        date_start: pd.Timestamp = None,
        date_end: pd.Timestamp = None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
        scalar_filter=None,
        columns: list[str] = None,
    ) -> Optional[ddf.DataFrame]:
        pass

    def estimate_datasets_size(
        self,
        uuid: str,
        keys: list[str] = None,
        date_start: pd.Timestamp = None,
        date_end: pd.Timestamp = None,
        multi_polygon=None,
        columns: list[str] = None,
        output_format: str = None,
    ) -> Optional[dict]:
        pass

    def get_api_status(self) -> bool:
        return False

    def map_column_names(
        self, uuid: str, key: str, columns: list[str] | None
    ) -> list[str] | None:
        pass

    def _extract_coordinate(
        self, data: dict, uuid: str, key: str, column: str
    ) -> Coordinate | None:

        if data is not None:
            mapped_col = self.map_column_names(uuid, key, [column])
            if mapped_col is None or len(mapped_col) == 0:
                return None
            # Translate to the correct column name for dataset
            val = data.get(mapped_col[0])

            if val is not None:
                return Coordinate(min=val.get("valid_min"), max=val.get("valid_max"))

        return None

    def normalize_to_0_360_if_needed(self, uuid: str, key: str, lon: float | None):
        """
        Normalize a longitude value to the range [0, 360], this happens with satellite data which is not [-180, 180]

        Parameters:
        lon (float): Longitude value (assume range -180, 180).

        Returns:
        float: Normalized longitude in [0, 360].
        """
        if lon is not None:
            if not -180 <= lon <= 180:
                raise TypeError(f"lon {lon} should be within -180, 180")

            desc: Descriptor = self.get_mapped_meta_data(uuid)[key]

            if desc is not None:
                if desc.lng.min == 0 and desc.lng.max == 360:
                    return lon + 180

        return lon

    @staticmethod
    def _extract_depth(data: dict) -> Depth | None:
        # We need to extract depth info
        depth = data.get("DEPTH")

        if depth is not None:
            return Depth(
                depth.get("valid_min"), depth.get("valid_max"), depth.get("units")
            )
        else:
            return None

    @staticmethod
    def normalize_lon(lon: float | None) -> float:
        if lon is None or -180 <= lon <= 180:
            return lon
        else:
            val = ((lon + 180) % 360) - 180
            # Handle special case where input 540 will become -180
            if val == -180 and lon > 180:
                return 180
            elif val == 180 and lon < -180:
                return -180
            else:
                return val

    @staticmethod
    def fix_encode_error_nested_dict(data):
        """
        The metadata may contain UTF-16 encode, this cause decode to json causing, source need to fix it
        but before that we need to work around the problem.
        :param data: Dict of dict where the content may have UTF-16
        :return: A clean dict with UTF-16 ignored
        """
        if isinstance(data, dict):
            return {
                key: BaseAPI.fix_encode_error_nested_dict(value)
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [BaseAPI.fix_encode_error_nested_dict(item) for item in data]
        elif isinstance(data, str):
            # Clean string by ignoring invalid surrogate characters
            return data.encode("utf-8", errors="ignore").decode("utf-8")
        else:
            return data

    @staticmethod
    def _calculate_chunk_sizes(
        sizes: Frozen[Hashable, int],
        dtype_size: int = 8,
        target_chunk_size_mb: float = 10,
    ) -> dict:
        """
        Calculate chunk sizes for each dimension to achieve a target chunk size in MB.

        Args:
            sizes: Dictionary of dimension names and their sizes (e.g., {'dim0': 388, 'dim1': 500, 'dim2': 500}).
            dtype_size: Size of the data type in bytes (e.g., 8 for float64, 4 for float32).
            target_chunk_size_mb: Target chunk size in MB (default: 50 MB).

        Returns:
            Dictionary of dimension names and their chunk sizes.
        """
        # Convert target chunk size to bytes
        target_chunk_size_bytes = target_chunk_size_mb * 1024 * 1024

        # Estimate available memory (use 50% of available RAM as a conservative limit)
        available_memory = psutil.virtual_memory().available * 0.5
        if target_chunk_size_bytes > available_memory:
            target_chunk_size_bytes = (
                available_memory / 10
            )  # Use 10% of available memory

        # Calculate total elements per chunk
        elements_per_chunk = target_chunk_size_bytes // dtype_size

        # Distribute elements across dimensions
        num_dims = len(sizes)
        if num_dims == 0:
            return {}

        # Start with equal distribution across dimensions
        dim_sizes = list(sizes.values())
        total_size = math.prod(dim_sizes)

        # Initialize chunk sizes
        chunk_sizes = {dim: size for dim, size in sizes.items()}

        if total_size <= elements_per_chunk:
            return chunk_sizes  # No chunking needed if dataset is small

        # Calculate chunk size per dimension (approximate equal split)
        elements_per_dim = int(elements_per_chunk ** (1 / num_dims))

        for dim, size in sizes.items():
            # Set chunk size to min of dimension size and calculated size
            chunk_sizes[dim] = min(size, max(1, elements_per_dim))

        # Adjust to ensure total chunk size is close to target
        current_elements = math.prod(chunk_sizes.values())
        if current_elements > elements_per_chunk:
            # Scale down largest dimension
            max_dim = max(sizes, key=lambda d: sizes[d])
            chunk_sizes[max_dim] = max(
                1, int(chunk_sizes[max_dim] * (elements_per_chunk / current_elements))
            )

        return chunk_sizes

    @staticmethod
    def zarr_to_dask_dataframe(
        dataset: xarray.Dataset,
        columns: Optional[List[str]] = None,
        chunks: Optional[Dict[str, int]] = None,
        target_chunk_size_mb: float = 200.0,
    ) -> ddf.DataFrame | None:
        """
        Convert an xarray Dataset to a Dask DataFrame with selected columns and adaptive chunking.

        This function handles large datasets efficiently by applying dynamic chunking based on
        the dimensions associated with the selected columns and automatically determining the
        number of partitions for the Dask DataFrame. Memory usage is kept within a target range
        (default: 200 MB per chunk). It works for any dataset with one or more dimensions in the
        selected columns, considering only the relevant dimensions for chunking and partitioning.
        All operations are lazy to avoid loading data into memory.

        Args:
            dataset: xarray.Dataset, potentially Zarr-backed.
            columns: List of variable names to select (default: all variables).
            chunks: Dictionary specifying chunk sizes for dimensions (e.g., {'dim1': 100}).
                    If None, chunk sizes are calculated automatically for dimensions in selected columns.
            target_chunk_size_mb: Target size for chunks in MB (default: 200 MB).

        Returns:
            dask.dataframe.DataFrame: DataFrame containing the specified columns, or None if dataset is empty.

        Raises:
            RuntimeError: If the dataset is not initialized.
            KeyError: If requested columns are not found in the dataset.
        """
        # Check if dataset is initialized
        if dataset is None:
            raise RuntimeError("Dataset not initialized")

        # Check if dataset is empty
        if not dataset.sizes or any(size == 0 for size in dataset.sizes.values()):
            return None

        # Get available variables
        available_columns = list(dataset.variables)

        # Use all variables if none specified
        columns = columns or available_columns

        # Validate requested columns
        if not all(col in available_columns for col in columns):
            missing_cols = [col for col in columns if col not in available_columns]
            raise KeyError(f"Columns {missing_cols} not found in {available_columns}")

        # Create a new Dataset with selected variables
        selected_data = {col: dataset[col] for col in columns}
        filtered_dataset = xarray.Dataset(selected_data)

        # Get dimensions associated with selected columns
        relevant_dims = set()
        for col in columns:
            relevant_dims.update(dataset[col].dims)
        relevant_dims = list(relevant_dims)

        # Get sizes of relevant dimensions
        dim_sizes = {dim: filtered_dataset.sizes.get(dim, 1) for dim in relevant_dims}

        # Apply chunking
        if chunks is None:
            # Estimate total size in bytes for selected data
            total_size_bytes = filtered_dataset.nbytes
            bytes_per_element = 8  # Assume float64 for estimation
            target_chunk_size_bytes = target_chunk_size_mb * 1e6
            target_elements = target_chunk_size_bytes // bytes_per_element

            # Calculate total elements
            total_elements = 1
            for size in dim_sizes.values():
                total_elements *= size

            # Calculate chunk sizes for relevant dimensions
            chunk_sizes = {}
            elements_per_chunk = min(target_elements, total_elements)
            if relevant_dims:
                # Distribute elements roughly evenly across relevant dimensions
                elements_per_dim = max(
                    1, int(elements_per_chunk ** (1 / len(relevant_dims)))
                )
                for dim in relevant_dims:
                    chunk_sizes[dim] = min(dim_sizes[dim], elements_per_dim)
            else:
                # Handle case where no dimensions are associated with selected columns
                chunk_sizes = {}
        else:
            # Apply user-specified chunks, but only for relevant dimensions
            chunk_sizes = {
                dim: size for dim, size in chunks.items() if dim in relevant_dims
            }

        filtered_dataset = filtered_dataset.chunk(chunk_sizes)

        # Estimate number of partitions based on chunking
        if relevant_dims:
            # Use the number of chunks along the first relevant dimension as a proxy
            first_dim = relevant_dims[0]
            dim_size = dim_sizes[first_dim]
            chunk_size = chunk_sizes.get(first_dim, dim_size)
            partition_size = max(1, dim_size // chunk_size)
        else:
            # For scalar variables, use a single partition
            partition_size = 1

        # Convert to Dask DataFrame
        df = filtered_dataset.to_dask_dataframe()

        # Reset index if coordinates are dimensions (to include them as columns)
        if any(dim in filtered_dataset.coords for dim in filtered_dataset.dims):
            df = df.reset_index()

        # Filter to requested columns
        df = df[columns]

        # Repartition the DataFrame to the estimated number of partitions
        # To cope with very restrict env, multiple partition by 2 to lower the memory
        if df.npartitions != partition_size:
            df = df.repartition(npartitions=partition_size * 2)

        return df


class API(BaseAPI):
    def __init__(self):
        # the ready flag used to check API status
        self._is_ready = False
        log.info("Init parquet data query instance")

        self._raw: Dict[str, Dict[str, Any]] = dict()
        self._cached_metadata: Dict[str, Dict[str, Descriptor]] = dict()

        # UUID to metadata mapper
        self._instance = DataQuery.GetAodn()
        self._metadata = None
        try:
            config = Config.get_config()

            temp_dir = os.path.join(os.getcwd(), ".duckdb_temp")
            os.makedirs(temp_dir, exist_ok=True)

            self._memconn = duckdb.connect(
                # use disk instead of memory as system low of memory and OOM
                # ":memory:cloud_optimized",
                "/tmp/wave_buoy.duckdb",
                config={
                    "threads": 1,
                    "temp_directory": temp_dir,
                    "max_memory": config.get_duckdb_maxmem(),
                },
            )
        except Exception as e:
            log.warning(f"Failed to set DuckDB memory/temp limits: {e}")

    def destroy(self):
        log.info("Destroying API instance")
        """
        Delete the temp file if it exists, this make sure Ngnix report something
        invalid and AWS knew process ends
        """
        if os.path.exists(HEALTH_JSON):
            try:
                os.remove(HEALTH_JSON)
            except OSError as e:
                # Do nothing as we end process
                pass

        self._memconn.close()

    async def async_initialize_metadata(self):
        # Use ThreadPoolExecutor to run blocking calls in a separate thread
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # Schedule the blocking calls in a thread
            await loop.run_in_executor(executor, lambda: self.initialize_metadata())

    def initialize_metadata(self):
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(HEALTH_JSON), exist_ok=True)

        """Write health status"""
        with open(HEALTH_JSON, "w") as f:
            f.write('{"status":"STARTING","status_code":200}')

        """Helper method to run blocking initialization tasks."""
        self._metadata = self._instance.get_metadata()
        self.refresh_uuid_dataset_map()

        # Free up catalog metadata memory and trigger garbage collection
        self._metadata = None
        gc.collect()

        process = psutil.Process()
        rss_mb = process.memory_info().rss / (1024 * 1024)
        vms_mb = process.memory_info().vms / (1024 * 1024)
        log.info(
            f"Done init. Memory usage: RSS = {rss_mb:.2f} MB, VMS = {vms_mb:.2f} MB"
        )
        # init finalised, set as ready
        self._is_ready = True

        with open(HEALTH_JSON, "w") as f:
            f.write('{"status":"UP","status_code":200}')

    def get_api_status(self) -> bool:
        # used for checking if the API instance is ready
        return self._is_ready

    def get_memconn(self):
        return self._memconn

    def get_aodn_instance(self):
        return self._instance

    def fetch_wave_buoy_data(self, buoy_name: str, start_date: str, end_date: str):
        buoy_name = unquote_plus(buoy_name)
        print("Fetching data for buoy:", buoy_name)
        waveBuoyPositionQueryResult = (
            self.get_memconn()
            .execute(
                f"""SELECT
            LATITUDE,
            LONGITUDE
            FROM wave_buoy_realtime_nonqc
            WHERE TIME >= '{start_date}' AND TIME < '{end_date}' AND site_name = '{buoy_name}' AND (WPFM IS NOT NULL OR WPMH IS NOT NULL) AND (WHTH IS NOT NULL OR WSSH IS NOT NULL)
            LIMIT 1"""
            )
            .df()
        )

        ds = ddf.from_pandas(waveBuoyPositionQueryResult)
        lat = (
            ds[STR_LATITUDE_UPPER_CASE].compute().values[0]
            if len(ds[STR_LATITUDE_UPPER_CASE].compute().values) > 0
            else None
        )
        lon = (
            ds[STR_LONGITUDE_UPPER_CASE].compute().values[0]
            if len(ds[STR_LONGITUDE_UPPER_CASE].compute().values) > 0
            else None
        )

        if lat is None or lon is None:
            return {}

        waveBuoyDataQueryResult = (
            self.get_memconn()
            .execute(
                f"""SELECT SSWMD, WPFM, WPMH, WHTH, WSSH, TIME
            FROM wave_buoy_realtime_nonqc
            WHERE TIME >= '{start_date}' AND TIME < '{end_date}' AND site_name = '{buoy_name}' AND (WPFM IS NOT NULL OR WPMH IS NOT NULL) AND (WHTH IS NOT NULL OR WSSH IS NOT NULL)
            ORDER BY TIME"""
            )
            .df()
        )
        feature = {
            "type": "Feature",
            "properties": {
                "SSWMD": [],
                "WPFM": [],
                "WPMH": [],
                "WHTH": [],
                "WSSH": [],
            },
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
        }

        for _, row in waveBuoyDataQueryResult.iterrows():
            # make it milliseconds since epoch, compatible with Highcharts, the time is in UTC because the data source is in UTC, so we can just use timestamp without timezone conversion
            time_sec = int(row["TIME"].timestamp() * 1000)
            if pd.notna(row["SSWMD"]):
                feature["properties"]["SSWMD"].append([time_sec, row["SSWMD"]])
            if pd.notna(row["WPFM"]):
                feature["properties"]["WPFM"].append([time_sec, row["WPFM"]])
            if pd.notna(row["WPMH"]):
                feature["properties"]["WPMH"].append([time_sec, row["WPMH"]])
            if pd.notna(row["WHTH"]):
                feature["properties"]["WHTH"].append([time_sec, row["WHTH"]])
            if pd.notna(row["WSSH"]):
                feature["properties"]["WSSH"].append([time_sec, row["WSSH"]])

        return feature

    def fetch_wave_buoy_latest_date(self):
        result = (
            self.get_memconn()
            .execute(
                f"""SELECT
            MAX(TIME) AS TIME
            FROM wave_buoy_realtime_nonqc"""
            )
            .df()
        )
        # DuckDB return pandas Timestamp which is timezone-naive like 2026-04-21 23:25:00, we need to convert it to ISO format with Z. The time is in UTC because the data source is in UTC, so we can just add Z at the end to indicate it is UTC time.
        return result["TIME"].item().isoformat() + "Z"

    def fetch_all_unique_wave_buoy_sites(self):
        result = (
            self.get_memconn()
            .execute(
                """SELECT
            site_name,
            MAX(TIME) AS TIME,
            first(LATITUDE) AS LATITUDE,
            first(LONGITUDE) AS LONGITUDE
            FROM wave_buoy_realtime_nonqc
            GROUP BY site_name"""
            )
            .df()
        )
        feature_collection = {
            "type": "FeatureCollection",
            "features": [],
        }
        for _, row in result.iterrows():
            feature = {
                "type": "Feature",
                "properties": {
                    "buoy": row["site_name"],
                    "date": row["TIME"].isoformat() + "Z",
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        row[STR_LONGITUDE_UPPER_CASE],
                        row[STR_LATITUDE_UPPER_CASE],
                    ],
                },
            }
            feature_collection["features"].append(feature)

        return feature_collection

    def fetch_wave_buoy_sites(self, start_date: str, end_date: str):
        result = (
            self.get_memconn()
            .execute(
                f"""SELECT
            site_name,
            first(TIME) AS TIME,
            first(LATITUDE) AS LATITUDE,
            first(LONGITUDE) AS LONGITUDE
            FROM wave_buoy_realtime_nonqc
            WHERE TIME >= '{start_date}' AND TIME < '{end_date}' AND (WPFM IS NOT NULL OR WPMH IS NOT NULL) AND (WHTH IS NOT NULL OR WSSH IS NOT NULL)
            GROUP BY site_name"""
            )
            .df()
        )
        feature_collection = {
            "type": "FeatureCollection",
            "features": [],
        }
        for _, row in result.iterrows():
            feature = {
                "type": "Feature",
                "properties": {
                    "buoy": row["site_name"],
                    # DuckDB return pandas Timestamp which is timezone-naive like 2026-04-21 23:25:00, we need to convert it to ISO format with Z. The time is in UTC because the data source is in UTC, so we can just add Z at the end to indicate it is UTC time.
                    "date": row["TIME"].isoformat() + "Z",
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        row[STR_LONGITUDE_UPPER_CASE],
                        row[STR_LATITUDE_UPPER_CASE],
                    ],
                },
            }
            feature_collection["features"].append(feature)

        return feature_collection

    # Do not use cache, so that we can refresh it again
    def refresh_uuid_dataset_map(self):
        # A map contains dataset name and Metadata class, which is not
        # so useful in our case, we need UUID
        catalog = self._metadata.metadata_catalog_uncached()
        if catalog == {}:
            log.error("Metadata catalog from cloud-optimised lib is empty.")

        for key in catalog:
            uuid = None
            try:
                data = catalog.get(key)
                uuid = API.get_metadata_uuid(data)

                if uuid is not None and uuid != "":
                    log.info("Adding uuid " + uuid + " name " + key)
                    if uuid not in self._raw:
                        self._raw[uuid] = dict()
                    # Save memory compress the content
                    self._raw[uuid][key] = zlib.compress(
                        json.dumps(data).encode("utf-8")
                    )

                    if uuid not in self._cached_metadata:
                        self._cached_metadata[uuid] = dict()
                    # We can add directly because the dict() created
                    self._cached_metadata[uuid][key] = Descriptor(
                        uuid=uuid,
                        dname=key,
                        lat=self._extract_coordinate(
                            data, uuid, key, STR_LATITUDE_UPPER_CASE
                        ),
                        lng=self._extract_coordinate(
                            data, uuid, key, STR_LONGITUDE_UPPER_CASE
                        ),
                        depth=BaseAPI._extract_depth(data),
                    )
                else:
                    log.error("Missing UUID entry for dataset " + key)
            except Exception as e:
                log.error(
                    "Failed to refresh UUID dataset map for dataset=%s uuid=%s. Error: %s",
                    key,
                    uuid,
                    e,
                )

    def get_mapped_meta_data(self, uuid: str | None):
        if uuid is not None:
            value = self._cached_metadata.get(uuid)
        else:
            # Return all values
            value = self._cached_metadata

        if value is not None:
            return value
        else:
            return {"not_exist": Descriptor(uuid=uuid if uuid is not None else "*")}

    def get_raw_meta_data(self, uuid: str) -> Dict[str, Any]:
        value = self._raw.get(uuid)

        if value is not None:
            decompressed = {
                key: json.loads(zlib.decompress(val).decode("utf-8"))
                for key, val in value.items()
            }
            return BaseAPI.fix_encode_error_nested_dict(decompressed)
        else:
            return dict()

    """
    Given a time range, we find if this uuid temporal cover the whole range
    """

    def has_data(
        self, uuid: str, key: str, start_date: pd.Timestamp, end_date: pd.Timestamp
    ):
        md: Dict[str, Descriptor] | None = self._cached_metadata.get(uuid)
        if md is not None and md[key] is not None:
            ds: DataQuery.DataSource = self._instance.get_dataset(md[key].dname)
            tes, tee = ds.get_temporal_extent()
            return start_date <= tes and tee <= end_date
        return False

    def get_temporal_extent(
        self, uuid: str, key: str
    ) -> Tuple[pd.Timestamp | None, pd.Timestamp | None]:
        md: Dict[str, Descriptor] | None = self._cached_metadata.get(uuid)
        if md is not None:
            ds: DataQuery.DataSource = self._instance.get_dataset(md[key].dname)
            start_date, end_date = ds.get_temporal_extent()

            if start_date is not None:
                start_date = start_date.replace(
                    hour=0, minute=0, second=0, microsecond=0, nanosecond=0
                )

            if end_date is not None:
                end_date = end_date.replace(
                    hour=23, minute=59, second=59, microsecond=999999, nanosecond=999
                )
            return start_date, end_date
        else:
            return None, None

    def map_column_names(
        self, uuid: str, key: str, columns: list[str] | None
    ) -> list[str] | None:

        if columns is None:
            return columns

        meta: Dict[str, Any] = self.get_raw_meta_data(uuid)[key]
        columns_map = Config.get_config().get_column_name_mapping()
        output = list()
        for column in columns:
            # If the column name can map to target data source column name
            if column.casefold() in columns_map:
                candidates = columns_map.get(column.casefold())
                if candidates is not None:
                    for candidate in candidates:
                        if candidate in meta:
                            output.append(candidate)
                            break
                    else:
                        log.error(
                            f"Column {column} is mapped to {candidates} but not in the metadata. "
                            f"Please check this dataset(uuid: {uuid}) if it is correct."
                        )
            else:
                output.append(column)

        return output

    def get_datasource(self, uuid: str, key: str) -> Optional[DataQuery.DataSource]:
        mds: Dict[str, Descriptor] | None = self._cached_metadata.get(uuid)
        if mds is not None and key in mds:
            md = mds[key]
            if md is not None:
                ds: DataQuery.DataSource = self._instance.get_dataset(md.dname)
                return ds
            else:
                return None
        else:
            return None

    def get_dataset(
        self,
        uuid: str,
        key: str,
        date_start: pd.Timestamp = None,
        date_end: pd.Timestamp = None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
        scalar_filter=None,
        columns: list[str] = None,
    ) -> Optional[ddf.DataFrame | xarray.Dataset]:
        """
        Get the data by calling cloud optimized data library aodn_cloud_optimised
        :param uuid: The UUID of the dataset
        :param key: Each UUID may have more than one dataset, this key is use to select the dataset you need
        :param date_start: Filter by start date
        :param date_end: Filter by end date
        :param lat_min: Filter by min lat, assume -90, 90
        :param lat_max: Filter by max lat, assume -90, 90
        :param lon_min: Filter by min lon, assume -180, 180
        :param lon_max: Filter by max lon, assume -180, 180
        :param scalar_filter: Additional filter not support by the argument
        :param columns: The column include in the return result, this is used to reduce unnecessary data flow
        :return: The dataset, noted zarr and parquest return different data type.
        """
        ds = self.get_datasource(uuid, key)

        if ds is not None:
            # Default get 10 days of data
            if date_start is None:
                date_start = (pd.Timestamp.now() - timedelta(days=10)).tz_convert("UTC")
            else:
                if date_start.tz is None:
                    raise ValueError("Missing timezone info in date_start")
                else:
                    date_start = pd.to_datetime(date_start).tz_convert(timezone.utc)

            if date_end is None:
                date_end = (
                    pd.Timestamp.now() + pd.offsets.Day(1) - pd.offsets.Nano(1)
                ).tz_convert("UTC")
            else:
                if date_end.tzinfo is None:
                    raise ValueError("Missing timezone info in date_end")
                else:
                    date_end = date_end.tz_convert(timezone.utc)

            # The get_data call the pyarrow and compare only works with non timezone datetime
            # now make sure the timezone is correctly convert to utc then remove it.
            # As get_date datetime are all utc, but the pyarrow do not support compare of datetime vs
            # datetime with timezone.
            if date_start.tz is not None:
                date_start = date_start.tz_localize(None)

            if date_end.tz is not None:
                date_end = date_end.tz_localize(None)

            # First, make sure lon is [-180, 180], some map application allow > 180
            lon_min = BaseAPI.normalize_lon(lon_min)
            lon_max = BaseAPI.normalize_lon(lon_max)

            # Now depends on dataset, especially satellite have convention [0,360] inside data
            # so we need to map it back to dataset specific coordinate
            lon_min = self.normalize_to_0_360_if_needed(uuid, key, lon_min)
            lon_max = self.normalize_to_0_360_if_needed(uuid, key, lon_max)

            try:
                # All precision to nanosecond
                if isinstance(ds, ParquetDataSource):
                    # map variable names
                    lat_mapped = self.map_column_names(
                        uuid, key, [STR_LATITUDE_UPPER_CASE]
                    )
                    lon_mapped = self.map_column_names(
                        uuid, key, [STR_LONGITUDE_UPPER_CASE]
                    )
                    time_mapped = self.map_column_names(
                        uuid, key, [STR_TIME_UPPER_CASE]
                    )
                    lat_varname = lat_mapped[0] if lat_mapped else None
                    lon_varname = lon_mapped[0] if lon_mapped else None
                    time_varname = time_mapped[0] if time_mapped else None

                    # Accuracy to nanoseconds
                    result = ds.get_data(
                        f"{date_start.strftime('%Y-%m-%d %H:%M:%S.%f')}{date_start.nanosecond:03d}",
                        f"{date_end.strftime('%Y-%m-%d %H:%M:%S.%f')}{date_end.nanosecond:03d}",
                        lat_min,
                        lat_max,
                        lon_min,
                        lon_max,
                        scalar_filter,
                        self.map_column_names(uuid, key, columns),
                        lat_varname=lat_varname,
                        lon_varname=lon_varname,
                        time_varname=time_varname,
                    )

                    return ddf.from_pandas(
                        result, npartitions=None, chunksize=None, sort=True
                    )
                elif isinstance(ds, ZarrDataSource):
                    # Lib slightly different for Zar file
                    return ds.get_data(
                        f"{date_start.strftime('%Y-%m-%d %H:%M:%S.%f')}{date_start.nanosecond:03d}",
                        f"{date_end.strftime('%Y-%m-%d %H:%M:%S.%f')}{date_end.nanosecond:03d}",
                        lat_min,
                        lat_max,
                        lon_min,
                        lon_max,
                        scalar_filter,
                    )
            except (ValueError, TypeError, IndexError, KeyError) as e:
                err_msg = str(e)
                # Some datasets (particularly secondary ones under a shared UUID)
                # have degenerate TIME coordinates (all-NaN or non-monotonic).
                # The upstream library raises this pandas slicing error in those cases.
                # Handle gracefully by returning no data instead of failing the request.
                if (
                    "non-monotonic" in err_msg
                    or "DatetimeIndex" in err_msg
                    or "partial slicing" in err_msg
                ):
                    log.warning(
                        "Dataset has unusable TIME coordinate, returning empty result for %s/%s: %s",
                        uuid,
                        key,
                        err_msg,
                    )
                    return None
                log.error(f"Error when query ds.get_data: {e}")
                raise e
            except Exception as v:
                log.error(f"Error when query ds.get_data: {v}")
                raise v
        else:
            return None

    @staticmethod
    def _timestamp_to_zarr_slice_str(ts: pd.Timestamp | None) -> str | None:
        """
        Convert a pandas Timestamp into the plain date string ZarrDataSource expects.

        ZarrDataSource.get_data slices the time coordinate with a string and cannot compare timezone-aware
        values, so we drop the tz (the values are already UTC by convention). Returns None unchanged
        so the slice stays open.
        """
        if ts is None:
            return None
        if ts.tz is not None:
            ts = ts.tz_convert(timezone.utc).tz_localize(None)
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    def estimate_datasets_size(
        self,
        uuid: str,
        keys: list[str] = None,
        date_start: pd.Timestamp = None,
        date_end: pd.Timestamp = None,
        multi_polygon=None,
        columns: list[str] = None,
        output_format: str = None,
    ) -> Optional[dict]:
        """
        Estimate the total download size across one or more keys of a dataset.

        :return: aggregated estimate dict, or None if no requested key exists
        :raises ValueError: if output_format is none or not supported
        """
        if output_format is None or output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"output_format must be one of {sorted(SUPPORTED_OUTPUT_FORMATS)}, "
                f"got '{output_format}'"
            )

        # Resolve keys, expanding "*" (or an absent list) to all keys of the
        # dataset - identical to the batch download (subsetting.py).
        if not keys or "*" in keys:
            keys = list((self.get_mapped_meta_data(uuid) or {}).keys())

        per_key: list[dict] = []
        missing: list[str] = []
        for key in keys:
            single = self._estimate_single_key_size(
                uuid,
                key,
                date_start=date_start,
                date_end=date_end,
                multi_polygon=multi_polygon,
                columns=columns,
                output_format=output_format,
            )
            if single is None:
                missing.append(key)
                continue
            per_key.append(single)

        if not per_key:
            # No requested key exists in this dataset -> 404 at the route.
            return None

        notes: list[str] = []
        if missing:
            notes.append(f"keys not found and skipped: {missing}")
        if len(per_key) > 1:
            notes.append(f"summed {len(per_key)} keys")

        return {
            "uuid": uuid,
            "keys": [r["key"] for r in per_key],
            "format": output_format,
            "estimated_uncompressed_bytes": sum(
                r["estimated_uncompressed_bytes"] for r in per_key
            ),
            "estimated_output_bytes": sum(r["estimated_output_bytes"] for r in per_key),
            "notes": "; ".join(notes),
            "per_key": per_key,
        }

    def _estimate_single_key_size(
        self,
        uuid: str,
        key: str,
        date_start: pd.Timestamp = None,
        date_end: pd.Timestamp = None,
        multi_polygon=None,
        columns: list[str] = None,
        output_format: str = "netcdf",
    ) -> Optional[dict]:
        """
        Estimate the download size of ONE key without downloading the data.

        :param uuid: Dataset UUID
        :param key: Dataset key (a UUID may map to several datasets)
        :param date_start: Filter start (UTC); None means open-ended
        :param date_end: Filter end (UTC); None means open-ended
        :param multi_polygon: GeoJSON MultiPolygon (str or object); None means
            no spatial filter (whole dataset)
        :param columns: If given, only these data variables count toward the size
        :param output_format: One of SUPPORTED_OUTPUT_FORMATS (netcdf/geotiff/csv)
        :return: A dict with the estimate, or None if the key is not found
        """
        ds = self.get_datasource(uuid, key)
        if ds is None:
            return None

        # Resolve the spatial filter into one or more bboxes. A multi_polygon is
        # reused via the same MultiPolygonHelper the batch download uses
        if multi_polygon is not None:
            from data_access_service.utils.multi_polygon_helper import (
                MultiPolygonHelper,
            )

            bboxes = MultiPolygonHelper(multi_polygon=multi_polygon).bboxes
            spatial_slices = [
                (
                    b.min_lat,
                    b.max_lat,
                    self.normalize_to_0_360_if_needed(
                        uuid, key, BaseAPI.normalize_lon(b.min_lon)
                    ),
                    self.normalize_to_0_360_if_needed(
                        uuid, key, BaseAPI.normalize_lon(b.max_lon)
                    ),
                )
                for b in bboxes
            ]
        else:
            # No spatial filter: a single open slice (whole dataset).
            spatial_slices = [(None, None, None, None)]

        if isinstance(ds, ZarrDataSource):
            return self._estimate_zarr_size(
                ds,
                uuid,
                key,
                date_start,
                date_end,
                spatial_slices,
                columns,
                output_format,
            )
        elif isinstance(ds, ParquetDataSource):
            # Parquet path uses pyarrow row-group metadata; added in a later step.
            raise NotImplementedError("Parquet size estimate is not implemented yet")
        else:
            return None

    def _estimate_zarr_size(
        self,
        ds: ZarrDataSource,
        uuid: str,
        key: str,
        date_start: pd.Timestamp | None,
        date_end: pd.Timestamp | None,
        spatial_slices: list[tuple],
        columns: list[str] | None,
        output_format: str,
    ) -> dict:
        if date_start is not None and date_end is not None:
            # Lazy import to avoid circular dependency
            from data_access_service.batch.subsetting_helper import (
                trim_date_range_for_keys,
            )

            date_start, date_end = trim_date_range_for_keys(
                self, uuid, [key], date_start, date_end
            )
            if date_start is None or date_end is None:
                # Requested range is entirely outside the dataset's extent: the
                # batch download emails "no data", the estimate reports zero size.
                return self._empty_estimate(uuid, key, output_format)

        date_start_str = self._timestamp_to_zarr_slice_str(date_start)
        date_end_str = self._timestamp_to_zarr_slice_str(date_end)

        notes: list[str] = []
        if len(spatial_slices) > 1:
            notes.append(
                f"summed {len(spatial_slices)} polygon bboxes "
                "(overlapping regions counted once per box -> upper bound)"
            )

        total_uncompressed = 0
        total_output = 0
        columns_note_done = False

        log.debug(
            "_estimate_zarr_size: uuid=%s key=%s slice=[%s..%s] bboxes=%d format=%s",
            uuid,
            key,
            date_start_str,
            date_end_str,
            len(spatial_slices),
            output_format,
        )

        for i, (la_min, la_max, lo_min, lo_max) in enumerate(spatial_slices):
            # get_data returns a lazily-sliced xarray.Dataset; chunks are NOT
            # loaded. We measure this one bbox and discard it before the next,
            # so no union grid is ever materialised.
            dataset: xarray.Dataset = ds.get_data(
                date_start_str, date_end_str, la_min, la_max, lo_min, lo_max
            )

            # Restrict to requested data variables, if any (reduces output size).
            # Columns subsetting not implemented yet but we want to account for it in the estimate
            if columns:
                mapped = self.map_column_names(uuid, key, columns) or []
                present = [c for c in mapped if c in dataset.data_vars]
                missing = [c for c in columns if c not in present]
                if present:
                    dataset = dataset[present]
                if missing and not columns_note_done:
                    notes.append(f"columns not found and ignored: {missing}")
            columns_note_done = True

            # .nbytes is metadata only (no compute); sum across bboxes.
            total_uncompressed += int(dataset.nbytes)

            # Output size depends on format - geotiff is NOT a flat multiplier on
            # nbytes so it has its own dimension-based estimate.
            if output_format == "geotiff":
                total_output += self._estimate_geotiff_output_bytes(
                    dataset, uuid, key, notes
                )
            else:
                ratio = OUTPUT_FORMAT_COMPRESSION_RATIO[output_format]
                total_output += int(int(dataset.nbytes) * ratio)

        if output_format != "geotiff":
            ratio = OUTPUT_FORMAT_COMPRESSION_RATIO[output_format]
            notes.append(f"compression ratio assumed: {ratio} for {output_format}")

        deduped_notes = list(dict.fromkeys(notes))

        log.debug(
            "_estimate_zarr_size: totals uncompressed=%d output=%d",
            total_uncompressed,
            total_output,
        )

        return {
            "uuid": uuid,
            "key": key,
            "format": output_format,
            "estimated_uncompressed_bytes": total_uncompressed,
            "estimated_output_bytes": total_output,
            "notes": "; ".join(deduped_notes),
        }

    @staticmethod
    def _empty_estimate(uuid: str, key: str, output_format: str) -> dict:
        """Zero-size estimate, returned when the requested range is outside the
        dataset's temporal extent (the batch download produces no data here)."""
        return {
            "uuid": uuid,
            "key": key,
            "format": output_format,
            "estimated_uncompressed_bytes": 0,
            "estimated_output_bytes": 0,
            "notes": "requested date range is outside the dataset's temporal extent",
        }

    def _estimate_geotiff_output_bytes(
        self,
        dataset: xarray.Dataset,
        uuid: str,
        key: str,
        notes: list[str],
    ) -> int:
        """
        Estimate GeoTIFF download size for a zarr selection

        GeoTIFF is not a flat ratio on nbytes. The real export writes one .tif per (eligible gridded variable x time step), each raster sized lat_size x lon_size, then bundles them into a ZIP. Only numeric variables that have BOTH the lat and
        lon dimensions are exported; everything else is dropped.

        estimated_output_bytes ~= sum_over_vars(n_time x lat x lon x bytes_per_pixel) x zip_ratio

        Falls back to the flat COMPRESSION_RATIO_GEOTIFF on nbytes only when no
        gridded variable is found at all (genuinely non-gridded data, e.g.
        point/timeseries).
        """
        lat_mapped = self.map_column_names(uuid, key, [STR_LATITUDE_UPPER_CASE]) or []
        lon_mapped = self.map_column_names(uuid, key, [STR_LONGITUDE_UPPER_CASE]) or []
        time_mapped = self.map_column_names(uuid, key, [STR_TIME_UPPER_CASE]) or []
        lat_name = lat_mapped[0] if lat_mapped else None
        lon_name = lon_mapped[0] if lon_mapped else None
        time_name = time_mapped[0] if time_mapped else None

        # curvilinear grids index by integer I/J instead of lon/lat. The real
        # exporter remaps I/J -> lat/lon before writing (subset_zarr.py).
        # But a size estimate only needs the cell COUNT so we don't do
        # conversion here.
        if "I" in dataset.dims and "J" in dataset.dims:
            lat_dim, lon_dim = "I", "J"
        else:
            lat_dim, lon_dim = lat_name, lon_name

        # Eligible = numeric variables gridded on both spatial dims, which
        # aligns with the exporter logic.
        eligible = [
            v
            for v in dataset.data_vars
            if v not in (lat_name, lon_name)
            and dataset[v].dtype.kind in ("i", "u", "f")
            and lat_dim in dataset[v].dims
            and lon_dim in dataset[v].dims
        ]
        if not eligible:
            notes.append(
                "geotiff: no gridded variables found, "
                f"fell back to flat ratio {COMPRESSION_RATIO_GEOTIFF}"
            )
            return int(dataset.nbytes * COMPRESSION_RATIO_GEOTIFF)

        lat_size = int(dataset.sizes.get(lat_dim, 1))
        lon_size = int(dataset.sizes.get(lon_dim, 1))
        n_time = int(dataset.sizes.get(time_name, 1)) if time_name else 1

        raw_raster_bytes = 0
        for v in eligible:
            # Integer rasters are cast to float32 before writing; floats keep
            # their own itemsize.
            kind = dataset[v].dtype.kind
            bytes_per_pixel = (
                GEOTIFF_INT_PIXEL_BYTES
                if kind in ("i", "u")
                else dataset[v].dtype.itemsize
            )
            raw_raster_bytes += n_time * lat_size * lon_size * bytes_per_pixel

        notes.append(
            f"geotiff: {len(eligible)} gridded var(s) x {n_time} time step(s), "
            f"grid {lat_size}x{lon_size}, zip ratio {GEOTIFF_ZIP_RATIO}"
        )
        return int(raw_raster_bytes * GEOTIFF_ZIP_RATIO)

    # TODO potential issue with UUID to dataset not 1 to 1
    @staticmethod
    def get_notebook_from(uuid: str) -> str:
        return get_notebook_url(uuid)

    @staticmethod
    def get_metadata_uuid(data: dict) -> str | None:
        if data.get("dataset_metadata") is not None:
            # For parquet data, uuid is found in here
            return data.get("dataset_metadata").get("metadata_uuid")
        elif data.get("global_attributes") is not None:
            # For zarr data, uuid is found in here
            return data.get("global_attributes").get("metadata_uuid")
        else:
            return None
