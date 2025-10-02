import asyncio
import gzip
import math
import duckdb

from concurrent.futures import ThreadPoolExecutor

import dask.dataframe as ddf
import pandas as pd
import logging
import xarray

from datetime import timedelta, timezone
from io import BytesIO
from typing import Optional, Dict, Any, List, Tuple, Hashable
from aodn_cloud_optimised import DataQuery
from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource, ZarrDataSource
from aodn_cloud_optimised.lib.config import get_notebook_url
from bokeh.server.tornado import psutil
from xarray.core.utils import Frozen
from data_access_service.core.descriptor import Depth, Descriptor
from urllib.parse import unquote_plus


log = logging.getLogger(__name__)


def _extract_depth(data: dict):
    # We need to extract depth info
    depth = data.get("DEPTH")

    if depth is not None:
        return Depth(depth.get("valid_min"), depth.get("valid_max"), depth.get("units"))
    else:
        return None


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

    def get_api_status(self) -> bool:
        return False

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
        self._is_ready = False
        self.memconn = duckdb.connect(":memory:cloud_optimized")

    def destroy(self):
        log.info("Destroying API instance")
        self.memconn.close()

    async def async_initialize_metadata(self):
        # Use ThreadPoolExecutor to run blocking calls in a separate thread
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # Schedule the blocking calls in a thread
            await loop.run_in_executor(executor, lambda: self.initialize_metadata())

    def initialize_metadata(self):
        """Helper method to run blocking initialization tasks."""
        self._metadata = self._instance.get_metadata()
        self.refresh_uuid_dataset_map()

        log.info("Done init")
        # init finalised, set as ready
        self._is_ready = True

    def get_api_status(self) -> bool:
        # used for checking if the API instance is ready
        return self._is_ready

    def fetch_wave_buoy_data(self, buoy_name: str, start_date: str, end_date: str):
        buoy_name = unquote_plus(buoy_name)
        print("Fetching data for buoy:", buoy_name)
        dataset = f"s3://{self._instance.bucket_name}/wave_buoy_realtime_nonqc.parquet"
        waveBuoyPositionQueryResult = self.memconn.execute(
            f"""SELECT
            LATITUDE,
            LONGITUDE
            FROM read_parquet('{dataset}/**/*.parquet', hive_partitioning=true)
            WHERE TIME >= '{start_date}' AND TIME < '{end_date}' AND site_name = '{buoy_name}' AND (SSWMD IS NOT NULL OR WPFM IS NOT NULL OR WSSH IS NOT NULL)
            LIMIT 1"""
        ).df()

        ds = ddf.from_pandas(waveBuoyPositionQueryResult)
        lat = (
            ds["LATITUDE"].compute().values[0]
            if len(ds["LATITUDE"].compute().values) > 0
            else None
        )
        lon = (
            ds["LONGITUDE"].compute().values[0]
            if len(ds["LONGITUDE"].compute().values) > 0
            else None
        )

        if lat is None or lon is None:
            return {}

        waveBuoyDataQueryResult = self.memconn.execute(
            f"""SELECT SSWMD, WPFM, WSSH, TIME
            FROM read_parquet('{dataset}/**/*.parquet', hive_partitioning=true)
            WHERE TIME >= '{start_date}' AND TIME < '{end_date}' AND site_name = '{buoy_name}' AND (SSWMD IS NOT NULL OR WPFM IS NOT NULL OR WSSH IS NOT NULL)
            ORDER BY TIME"""
        ).df()
        feature = {
            "type": "Feature",
            "properties": {
                "SSWMD": [],
                "WPFM": [],
                "WSSH": [],
            },
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
        }

        for _, row in waveBuoyDataQueryResult.iterrows():
            time_sec = int(row["TIME"].timestamp() * 1000)
            feature["properties"]["SSWMD"].append([time_sec, row["SSWMD"]])
            feature["properties"]["WPFM"].append([time_sec, row["WPFM"]])
            feature["properties"]["WSSH"].append([time_sec, row["WSSH"]])

        return feature

    def fetch_wave_buoy_sites(self, start_date: str, end_date: str):
        dataset = f"s3://{self._instance.bucket_name}/wave_buoy_realtime_nonqc.parquet"
        result = self.memconn.execute(
            f"""SELECT
            site_name,
            first(TIME) AS TIME,
            first(LATITUDE) AS LATITUDE,
            first(LONGITUDE) AS LONGITUDE
            FROM read_parquet('{dataset}/**/*.parquet', hive_partitioning=true)
            WHERE TIME >= '{start_date}' AND TIME < '{end_date}' AND (SSWMD IS NOT NULL OR WPFM IS NOT NULL OR WSSH IS NOT NULL)
            GROUP BY site_name"""
        ).df()
        feature_collection = {
            "type": "FeatureCollection",
            "features": [],
        }
        DATE_FORMAT = "%Y-%m-%d"
        for _, row in result.iterrows():
            feature = {
                "type": "Feature",
                "properties": {
                    "buoy": row["site_name"],
                    "date": row["TIME"].strftime(DATE_FORMAT),
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["LONGITUDE"], row["LATITUDE"]],
                },
            }
            feature_collection["features"].append(feature)

        return feature_collection

    # Do not use cache, so that we can refresh it again
    def refresh_uuid_dataset_map(self):
        # A map contains dataset name and Metadata class, which is not
        # so useful in our case, we need UUID
        catalog = self._metadata.metadata_catalog_uncached()

        for key in catalog:
            data = catalog.get(key)
            uuid = API.get_metadata_uuid(data)

            if uuid is not None and uuid != "":
                log.info("Adding uuid " + uuid + " name " + key)
                if uuid not in self._raw:
                    self._raw[uuid] = dict()
                # We can add directly because the dict() created
                self._raw[uuid][key] = data

                if uuid not in self._cached_metadata:
                    self._cached_metadata[uuid] = dict()
                # We can add directly because the dict() created
                self._cached_metadata[uuid][key] = Descriptor(
                    uuid=uuid, dname=key, depth=_extract_depth(data)
                )
            else:
                log.error("Mising UUID entry for dataset " + key)

    def get_mapped_meta_data(self, uuid: str | None):
        if uuid is not None:
            value = self._cached_metadata.get(uuid)
        else:
            # Return all values
            value = self._cached_metadata

        if value is not None:
            return value
        else:
            return {"not_exist": Descriptor(uuid=uuid)}

    def get_raw_meta_data(self, uuid: str) -> Dict[str, Any]:
        value = self._raw.get(uuid)

        if value is not None:
            return value
        else:
            return dict()

    """
    Given a time range, we find if this uuid temporal cover the whole range
    """

    def has_data(
        self, uuid: str, key: str, start_date: pd.Timestamp, end_date: pd.Timestamp
    ):
        md: Dict[str, Descriptor] = self._cached_metadata.get(uuid)
        if md is not None and md[key] is not None:
            ds: DataQuery.DataSource = self._instance.get_dataset(md[key].dname)
            tes, tee = ds.get_temporal_extent()
            return start_date <= tes and tee <= end_date
        return False

    def get_temporal_extent(
        self, uuid: str, key: str
    ) -> Tuple[pd.Timestamp | None, pd.Timestamp | None]:
        md: Dict[str, Descriptor] = self._cached_metadata.get(uuid)
        if md is not None:
            ds: DataQuery.DataSource = self._instance.get_dataset(md[key].dname)
            start_date, end_date = ds.get_temporal_extent()
            start_date.replace(hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
            end_date.replace(
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
        output = list()
        for column in columns:

            # You want TIME field but not in there, try map to something else
            if column.casefold() == "TIME".casefold() and (
                "TIME" not in meta or "time" not in meta
            ):
                match meta:
                    case meta if "JULD" in meta:
                        output.append("JULD")
                    case meta if "detection_timestamp" in meta:
                        output.append("detection_timestamp")
                    case meta if "TIME" in meta:
                        output.append("TIME")
                    case meta if "time" in meta:
                        output.append("time")
                    case meta if "timestamp" in meta:
                        log.error(
                            f"For most datasets, timestamp should not be the field to express the accurate time. "
                            f"Please check this dataset(uuid: {uuid}) if it is correct."
                        )
                        output.append("timestamp")

            # You want depth field, but it is not in data
            elif column.casefold() == "DEPTH".casefold() and (
                "DEPTH" not in meta or "depth" not in meta
            ):
                # Just ignore the field in the query, assume zero
                pass
            elif column.casefold() == "LATITUDE".casefold() and (
                "LATITUDE" not in meta or "latitude" not in meta
            ):
                match meta:
                    case meta if "latitude" in meta:
                        output.append("latitude")
                    case meta if "LATITUDE" in meta:
                        output.append("LATITUDE")
                    case meta if "lat" in meta:
                        output.append("lat")
            elif column.casefold() == "LONGITUDE".casefold() and (
                "LONGITUDE" not in meta or "longitude" not in meta
            ):
                match meta:
                    case meta if "longitude" in meta:
                        output.append("longitude")
                    case meta if "LONGITUDE" in meta:
                        output.append("LONGITUDE")
                    case meta if "lon" in meta:
                        output.append("lon")
            else:
                output.append(column)

        return output

    def get_datasource(self, uuid: str, key: str) -> Optional[DataQuery.DataSource]:
        mds: Dict[str, Descriptor] = self._cached_metadata.get(uuid)
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

            try:
                # All precision to nanosecond
                if isinstance(ds, ParquetDataSource):
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
            except ValueError as e:
                log.error(f"Error when query ds.get_data: {e}")
                raise e
            except Exception as v:
                log.error(f"Error when query ds.get_data: {v}")
                raise
        else:
            return None

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
