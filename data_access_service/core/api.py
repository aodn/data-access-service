import asyncio
import gzip
import math
import os
import gc

import json
import zlib


from concurrent.futures import ThreadPoolExecutor
from data_access_service import Config

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
    STR_LONGITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)
from data_access_service.models.subset_request import (
    NON_SPECIFIED,
    SUPPORTED_OUTPUT_FORMATS,
)
from data_access_service.core.size_estimation import estimate_single_key_size
from data_access_service.utils.subsetting_resolver import resolve_subset
from data_access_service.core.descriptor import Depth, Descriptor, Coordinate

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
        start_date: str = NON_SPECIFIED,
        end_date: str = NON_SPECIFIED,
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

    def get_aodn_instance(self):
        return self._instance

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
                # Some dataset has future date, we need to do a safety check
                if end_date.tzinfo is None:
                    now_compare = pd.Timestamp.now()
                else:
                    now_compare = pd.Timestamp.now(tz="UTC")
                    end_date = end_date.tz_convert("UTC")

                end_date = end_date.replace(
                    hour=23, minute=59, second=59, microsecond=999999, nanosecond=999
                )

                if end_date > now_compare:
                    end_date = now_compare
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

    def estimate_datasets_size(
        self,
        uuid: str,
        keys: list[str] = None,
        start_date: str = NON_SPECIFIED,
        end_date: str = NON_SPECIFIED,
        multi_polygon=None,
        columns: list[str] = None,
        output_format: str = None,
    ) -> Optional[dict]:
        """
        Estimate the total download size across one or more keys of a dataset.

        The request is interpreted by the same resolve_subset the batch
        download uses (key expansion, date defaults + extent trim, bboxes)

        :return: aggregated estimate dict, or None if no requested key exists
        :raises ValueError: if output_format is none or not supported, or the
            dates are unparseable
        """
        if output_format is None or output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"output_format must be one of {sorted(SUPPORTED_OUTPUT_FORMATS)}, "
                f"got '{output_format}'"
            )

        resolved_subset = resolve_subset(
            api=self,
            uuid=uuid,
            keys=keys,
            start_date_str=start_date,
            end_date_str=end_date,
            multi_polygon=multi_polygon,
            columns=columns,
        )

        per_key: list[dict] = []
        missing: list[str] = []
        for key in resolved_subset.keys:
            single = estimate_single_key_size(
                self, key, resolved_subset, output_format=output_format
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

        # Todo: we may not need so many fields, but keep it for now for debugging
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
