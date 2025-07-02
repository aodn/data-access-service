import asyncio
import gzip
from concurrent.futures import ThreadPoolExecutor

import dask.dataframe as ddf
import pandas as pd
import logging

from datetime import timedelta, datetime, timezone
from io import BytesIO
from typing import Optional, Dict, Any, List

import xarray
from aodn_cloud_optimised import DataQuery
from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource, ZarrDataSource
from aodn_cloud_optimised.lib.config import get_notebook_url

from data_access_service.core.descriptor import Depth, Descriptor

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
    def get_temporal_extent(self, uuid: str, key: str) -> (datetime, datetime):
        pass

    def get_mapped_meta_data(self, uuid: str | None) -> Dict[str, Descriptor]:
        pass

    def has_data(self, uuid: str, key: str, start_date: datetime, end_date: datetime):
        pass

    def get_dataset_data(
        self,
        uuid: str,
        key: str,
        date_start: datetime = None,
        date_end: datetime = None,
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
    def zarr_to_dask_dataframe(
        dataset: xarray.Dataset, columns: Optional[List[str]] = None
    ) -> ddf.DataFrame | None:
        """
        Filter an xarray.Dataset to specific columns (variables) and convert to a Pandas DataFrame.

        Args:
            dataset: xarray.Dataset, potentially Zarr-backed.
            columns: List of variable names to select (default: all variables).
            npartitions: size

        Returns:
            pandas.DataFrame: DataFrame containing the specified columns.

        Raises:
            RuntimeError: If the dataset is not initialized.
            KeyError: If requested columns are not found.
        """
        # Check if dataset is initialized
        if dataset is None:
            raise RuntimeError("Dataset not initialized")

        # Short circuit here, it is much memory efficient to check size before
        # convert it to dask. After convert to dask, if you want to check size
        # it force data load and in most case takes all memory can result in
        # MemoryError exception
        if not dataset.sizes or any(size == 0 for size in dataset.sizes.values()):
            return None

        # Get available variables
        available_columns = list(dataset.variables)

        # Use all variables if none specified
        columns = columns or available_columns

        # Validate requested columns
        if not all(col in available_columns for col in columns):
            raise KeyError(f"Some columns {columns} not found in {available_columns}")

        # Create a new Dataset with selected variables and promote coordinates if needed
        selected_data = {}
        for col in columns:
            if col in dataset.variables:
                selected_data[col] = dataset[col]

        # Select specified variables (returns a new Dataset)
        filtered_dataset = xarray.Dataset(selected_data)

        # Convert to Dask DataFrame
        df = filtered_dataset.to_dask_dataframe()

        # Reset index to include coordinates as columns
        df = df.reset_index()

        # Filter to requested columns
        df = df[columns]

        return df


class API(BaseAPI):
    def __init__(self):
        # the ready flag used to check API status
        self._is_ready = False
        log.info("Init parquet data query instance")

        self._raw: Dict[str, Dict[str, Any]] = dict()
        self._cached: Dict[str, Dict[str, Descriptor]] = dict()

        # UUID to metadata mapper
        self._instance = DataQuery.GetAodn()
        self._metadata = None
        self._is_ready = False

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

                if uuid not in self._cached:
                    self._cached[uuid] = dict()
                # We can add directly because the dict() created
                self._cached[uuid][key] = Descriptor(
                    uuid=uuid, dname=key, depth=_extract_depth(data)
                )
            else:
                log.error("Mising UUID entry for dataset " + key)

    def get_mapped_meta_data(self, uuid: str | None) -> Dict[str, Descriptor]:
        if uuid is not None:
            value = self._cached.get(uuid)
        else:
            # Return all values
            value = self._cached

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

    def has_data(self, uuid: str, key: str, start_date: datetime, end_date: datetime):
        md: Dict[str, Descriptor] = self._cached.get(uuid)
        if md is not None and md[key] is not None:
            ds: DataQuery.DataSource = self._instance.get_dataset(md[key].dname)
            te = ds.get_temporal_extent()
            return start_date <= te[0] and te[1] <= end_date
        return False

    def get_temporal_extent(self, uuid: str, key: str) -> (datetime, datetime):
        md: Dict[str, Descriptor] = self._cached.get(uuid)
        if md is not None:
            ds: DataQuery.DataSource = self._instance.get_dataset(md[key].dname)
            return ds.get_temporal_extent()
        else:
            return ()

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
                    case meta if "timestamp" in meta:
                        output.append("timestamp")
                    case meta if "TIME" in meta:
                        output.append("TIME")
                    case meta if "time" in meta:
                        output.append("time")
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
            elif column.casefold() == "LONGITUDE".casefold() and (
                "LONGITUDE" not in meta or "longitude" not in meta
            ):
                match meta:
                    case meta if "longitude" in meta:
                        output.append("longitude")
                    case meta if "LONGITUDE" in meta:
                        output.append("LONGITUDE")
            else:
                output.append(column)

        return output

    def get_dataset_data(
        self,
        uuid: str,
        key: str,
        date_start: datetime = None,
        date_end: datetime = None,
        lat_min=None,
        lat_max=None,
        lon_min=None,
        lon_max=None,
        scalar_filter=None,
        columns: list[str] = None,
    ) -> Optional[ddf.DataFrame | xarray.Dataset]:
        mds: Dict[str, Descriptor] = self._cached.get(uuid)

        if mds is not None and key in mds:
            md = mds[key]
            if md is not None:
                ds: DataQuery.DataSource = self._instance.get_dataset(md.dname)

                # Default get 10 days of data
                if date_start is None:
                    date_start = datetime.now(timezone.utc) - timedelta(days=10)
                else:
                    if date_start.tzinfo is None:
                        date_start = pd.to_datetime(date_start).tz_localize(
                            timezone.utc
                        )
                    else:
                        date_start = pd.to_datetime(date_start).tz_convert(timezone.utc)

                if date_end is None:
                    date_end = datetime.now(timezone.utc)
                else:
                    if date_end.tzinfo is None:
                        date_end = pd.to_datetime(date_end).tz_localize(timezone.utc)
                    else:
                        date_end = pd.to_datetime(date_end).tz_convert(timezone.utc)

                # The get_data call the pyarrow and compare only works with non timezone datetime
                # now make sure the timezone is correctly convert to utc then remove it.
                # As get_date datetime are all utc, but the pyarrow do not support compare of datetime vs
                # datetime with timezone.
                if date_start.tzinfo is not None:
                    date_start = date_start.astimezone(timezone.utc).replace(
                        tzinfo=None
                    )

                if date_end.tzinfo is not None:
                    date_end = date_end.astimezone(timezone.utc).replace(tzinfo=None)

                try:
                    if isinstance(ds, ParquetDataSource):
                        result = ds.get_data(
                            str(date_start),
                            str(date_end),
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
                            str(date_start),
                            str(date_end),
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
