import math
import os
import tempfile
from pathlib import Path
from typing import List

import gc
import time
import dask
import numcodecs
import psutil
import resource
import xarray

import numpy as np
import dask.array as da
from xarray import DataArray

from data_access_service import init_log, Config, API
from data_access_service.batch.subsetting_helper import trim_date_range_for_keys
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.constants import (
    STR_TIME_UPPER_CASE,
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
)
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.multi_polygon_helper import MultiPolygonHelper
from data_access_service.utils.date_time_utils import supply_day_with_nano_precision
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.process_logger import ProcessLogger


class ZarrProcessor:
    def __init__(
        self,
        api: API,
        uuid: str,
        job_id: str,
        keys: List[str],
        start_date_str: str,
        end_date_str: str,
        multi_polygon: str,
        recipient: str,
        collection_title: str,
        full_metadata_link: str,
        suggested_citation: str,
    ):
        self.aws = AWSHelper()
        self.api = api
        self.config = Config.get_config()
        self.log = init_log(self.config)
        self.job_id = job_id
        self.uuid = uuid
        self.recipient = recipient

        start_date, end_date = supply_day_with_nano_precision(
            start_date_str=start_date_str,
            end_date_str=end_date_str,
        )

        if "*" in keys:
            md = self.api.get_mapped_meta_data(self.uuid)
            self.keys = list(md.keys())
        else:
            self.keys = keys

        trimmed_start_date, trimmed_end_date = trim_date_range_for_keys(
            api=api,
            uuid=uuid,
            keys=self.keys,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
        if trimmed_start_date is None or trimmed_end_date is None:
            # requested date range does not overlap with data available
            text_body = (
                f"No data available for your subset request for dataset {uuid} with keys {keys} "
                f"and date range from {start_date_str} to {end_date_str}."
                f"and selected area is {multi_polygon}."
            )
            AWSHelper().send_email(
                recipient=recipient,
                subject="No Data Available for Your Subset Request",
                text_body=text_body,
            )
            return

        self.start_date = trimmed_start_date
        self.end_date = trimmed_end_date
        self.multi_polygon = MultiPolygonHelper(multi_polygon=multi_polygon)
        self.bboxes = self.multi_polygon.bboxes

        self.subset_request = SubsetRequest(
            uuid=self.uuid,
            keys=self.keys,
            start_date=start_date_str,
            end_date=end_date_str,
            bboxes=self.bboxes,
            recipient=self.recipient,
            collection_title=collection_title,
            full_metadata_link=full_metadata_link,
            suggested_citation=suggested_citation,
        )

    def process(self):
        self.log.info(
            f"Start processing zarr data for uuid: {self.uuid}, job id: {self.job_id}"
        )
        urls: List[str] = []

        self.log.info("Datasets to process: %s", self.keys)
        for key in self.keys:
            self.log.info("Processing dataset: %s", key)
            dataset = self.__get_zarr_dataset_for_(key=key)
            if dataset is None:
                self.log.info("No data found for dataset: %s, skipping...", key)
                continue

            download_uri = self.__write_to_s3_as_netcdf(dataset=dataset, key=key)
            urls.append(download_uri)
        subject = f"Finish processing data file whose uuid is:  {self.uuid}"

        html_content = get_download_email_html_body(
            subset_request=self.subset_request, object_urls=urls
        )

        self.aws.send_email(
            recipient=self.recipient, subject=subject, html_body=html_content
        )

    def __get_zarr_dataset_for_(self, key: str) -> xarray.Dataset | None:

        # apply subsetting conditions for each bbox and merge them
        merged_dataset: xarray.Dataset | None = None
        for bbox in self.bboxes:

            dataset = self.api._instance.get_dataset(key).zarr_store

            conditions = self.get_all_subset_conditions(key, bbox)

            time_dim = self.api.map_column_names(
                uuid=self.uuid, key=key, columns=[STR_TIME_UPPER_CASE]
            )[0]
            time_per_chunk = self.__get_time_steps_per_chunk(dataset, time_dim)
            self.log.info(
                "Chunking dataset with %d time steps per chunk", time_per_chunk
            )
            dataset = dataset.chunk({time_dim: time_per_chunk})
            dim_conditions = {}  # Conditions for dimensions of zarr
            mask = None  # Conditions for variables (include coord and data_var) of zarr. Naming "mask" is for zarr convention

            # conditions (time range, lat range, lon range etc.) may be dimensions or variables for different zarr datasets
            # Below loop forms the conditions accordingly
            # if it is a dimension, it will be added to dim_conditions for sel()
            # if it is a variable, it will be used to form a mask for where()
            for k, val_range in conditions.items():
                print("forming condition for key", k, "with range", val_range)
                if is_dim(key=k, dataset=dataset):
                    form_dim_conditions(
                        existing_conditions=dim_conditions,
                        key=k,
                        min_value=val_range[0],
                        max_value=val_range[1],
                        dataset=dataset,
                    )
                elif is_var(key=k, dataset=dataset):
                    mask = form_mask(
                        existing_mask=mask,
                        key=k,
                        min_value=val_range[0],
                        max_value=val_range[1],
                        ds=dataset,
                    )

                else:
                    raise ValueError(
                        f"Condition key: {k} is neither dim, coord nor data_var in the dataset. Dataset: {key}"
                    )

            # apply dim conditions and mask (variable conditions)
            subset = dataset
            if dim_conditions:
                subset = subset.sel(**dim_conditions)
            if mask is not None:
                # please use drop=False to keep lazy loading
                subset = subset.where(mask, drop=False)

            if not isinstance(subset, xarray.Dataset):
                raise TypeError(
                    f"Data for key: {key} is not an xarray.Dataset. This only support zarr format."
                )

            if merged_dataset is None:
                merged_dataset = subset
            else:
                merged_dataset = xarray.merge(
                    [merged_dataset, subset], compat="override"
                )

        if merged_dataset is None:
            self.log.warning(
                f"No data found for key: {key} in the specified conditions."
            )
            return None
        return merged_dataset

    def __write_to_s3_as_netcdf(self, dataset: xarray.Dataset, key: str):
        dataset = ignore_invalid_unicode_in_attrs(dataset)

        # Convert object dtype variables to appropriate fixed-size strings, because NetCDF does not support object dtype
        dataset = convert_object_dtype_variables(dataset, logger=self.log)

        time_dim = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_TIME_UPPER_CASE]
        )[0]
        time_per_chunk = self.__get_time_steps_per_chunk(dataset, time_dim)
        self.log.info("Chunking dataset with %d time steps per chunk", time_per_chunk)
        dataset = dataset.chunk({time_dim: time_per_chunk})
        netcdf_compression = {
            var: {"zlib": True, "complevel": 5}
            for var, da in dataset.data_vars.items()
            if da.dtype.kind in {"i", "u", "f"}  # integer, unsigned, float
        }
        thread_count = self.get_available_thread_count()

        # set the thread count for dask, for the to_netcdf operation later
        dask.config.set(num_workers=thread_count)
        bucket_name = self.config.get_csv_bucket_name()

        # Free memory before to_netcdf to prevent allocation errors.
        # Don't skip this step even though most of the situations no memory is freed,
        # There might be some edge cases that memory is not well managed.
        # So it is safer to keep this step here.
        self.log.info("Freeing memory before to_netcdf()...")
        mem_before = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
        self.log.info(f"  Memory before cleanup: {mem_before:.2f} GB")
        # Force garbage collection to free unused memory
        gc.collect()
        # Give Python a moment to release memory back to OS
        time.sleep(0.5)

        memory_after = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
        self.log.info(f"  Memory after cleanup: {memory_after:.2f} GB")
        self.log.info(f"  Memory freed: {(mem_before - memory_after):.2f} GB")

        with tempfile.TemporaryDirectory() as tempdirname:
            # with ProcessLogger(logger=self.log, task_name="Writing to s3 as netcdf"):
            temp_netcdf_path = Path(tempdirname) / key.replace(".zarr", ".nc")

            # Write variables one by one to manage memory better
            data_var_names = list(dataset.data_vars.keys())
            self.log.info(
                f"Writing {len(data_var_names)} variables incrementally: {data_var_names}"
            )

            with ProcessLogger(
                logger=self.log, task_name="Step 1: Writing coordinates to NetCDF file"
            ):
                # First write: Create file with coordinates only (no data variables)
                coords_only_ds = xarray.Dataset(
                    coords=dataset.coords, attrs=dataset.attrs
                )

                # Compute coordinates to avoid OOM during write
                coords_only_ds = coords_only_ds.compute()

                coords_only_ds.to_netcdf(
                    temp_netcdf_path,
                    mode="w",
                    engine="netcdf4",
                    format="NETCDF4",
                    compute=True,
                )

                # Clean up after writing coordinates to avoid edge case unexpected memory management
                del coords_only_ds
                gc.collect()

            # Append each data variable one by one since xr.Dataset.to_netcdf() only supports mode "w" and "a"
            # and "a" mode appends to existing file will overwrite existing variables.
            # doc: https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_netcdf.html
            for idx, var_name in enumerate(data_var_names, start=1):
                with ProcessLogger(
                    logger=self.log,
                    task_name=f"Step {idx + 1}/{len(data_var_names) + 1}: Appending variable '{var_name}'",
                ):

                    # Create a dataset with only this variable
                    # CRITICAL: Do NOT include coords when appending - they already exist in the file
                    single_var_ds = xarray.Dataset({var_name: dataset[var_name]})

                    # Apply compression encoding for this variable if applicable
                    var_encoding = {}
                    if var_name in netcdf_compression:
                        var_encoding[var_name] = netcdf_compression[var_name]

                    # Append this variable to the existing file
                    single_var_ds.to_netcdf(
                        temp_netcdf_path,
                        mode="a",
                        engine="netcdf4",
                        format="NETCDF4",
                        encoding=var_encoding,
                        compute=True,
                    )

                    # Clean up after each variable
                    del single_var_ds
                    gc.collect()

                    # Log memory usage
                    current_mem = psutil.Process(os.getpid()).memory_info().rss / (
                        1024**3
                    )
                    self.log.info(
                        f"  Memory after writing '{var_name}': {current_mem:.2f} GB"
                    )

            self.log.info("All variables written successfully")

            s3_key = f"{self.job_id}/{key.replace('.zarr', '.nc')}"
            self.log.info(
                "Start uploading to s3 bucket: %s, key: %s", bucket_name, s3_key
            )
            self.aws.upload_file_to_s3(str(temp_netcdf_path), bucket_name, s3_key)
            region = self.aws.s3.meta.region_name
            return f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

    def get_available_thread_count(self):
        if os.getenv("PROFILE") in (None, "dev", "testing"):
            self.log.info("Running in dev or testing mode, using 1 thread")
            return 1

        cpu_count = psutil.cpu_count(logical=True)
        self.log.info(f"Available thread count: {cpu_count}")
        return cpu_count

    def __get_time_steps_per_chunk(
        self, dataset: xarray.Dataset, time_dim: str, memory_fraction: float = 0.1
    ) -> int:
        """
        Calculate the number of time steps per chunk based on available memory and dataset size.
        This helps to optimize memory usage during processing.
        the memory_fraction is the fraction of available memory to use for processing. The
        value is only for safety. Can be adjusted based on the experience.
        """
        available_memory = psutil.virtual_memory().available
        self.log.info(
            "total memory in MB: %d", psutil.virtual_memory().total / (1024 * 1024)
        )
        self.log.info(f"Available memory in MB: {available_memory / (1024 * 1024):.2f}")
        safe_memory_per_thread = int(
            available_memory * memory_fraction / self.get_available_thread_count()
        )
        self.log.info(
            "Chunk size: %d MB per thread", safe_memory_per_thread / (1024**2)
        )

        # var.nbytes forces computation - use size * itemsize instead
        estimated_size = 0
        for var_name, var_data in dataset.data_vars.items():
            if hasattr(var_data, "dtype") and hasattr(var_data, "size"):
                estimated_size += var_data.size * var_data.dtype.itemsize

        # Fallback: use conservative estimate based on dimensions
        if estimated_size == 0:
            total_elements = 1
            for dim_size in dataset.sizes.values():
                total_elements *= dim_size
            # Assume average 8 bytes per element (float64)
            estimated_size = total_elements * 8

        self.log.info(f"Estimated dataset size: {estimated_size / (1024**3):.2f} GB")
        chunk_count = max(1, math.ceil(estimated_size / safe_memory_per_thread))
        self.log.info("Chunk count: %d", chunk_count)
        total_time_count = dataset.sizes[time_dim]
        return math.ceil(total_time_count / chunk_count)

    def get_all_subset_conditions(self, key: str, bbox: BoundingBox) -> dict[str, list]:
        # Please add more conditions if they are supported in the future
        time_dim = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_TIME_UPPER_CASE]
        )[0]
        lat_dim = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_LATITUDE_UPPER_CASE]
        )[0]
        lon_dim = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_LONGITUDE_UPPER_CASE]
        )[0]

        return {
            time_dim: [
                self.start_date.tz_convert("UTC").tz_localize(None),
                self.end_date.tz_convert("UTC").tz_localize(None),
            ],
            lat_dim: [bbox.min_lat, bbox.max_lat],
            lon_dim: [bbox.min_lon, bbox.max_lon],
        }


def convert_object_dtype_variables(dataset: xarray.Dataset, logger) -> xarray.Dataset:
    """
    Convert object dtype variables to fixed-size string dtype by:
    1. Loading data in chunks to determine max string length (memory-safe)
    2. Converting to appropriate fixed-size dtype (e.g., S64, S128)

    This avoids the SerializationWarning and prevents sudden memory allocation
    when saving to NetCDF.
    """

    for var_name in list(dataset.variables.keys()):
        var = dataset[var_name]

        # Check if variable has object dtype
        if var.dtype != np.dtype("object"):
            continue

        logger.info(f"Processing object dtype variable: {var_name}")
        logger.info(f"  Shape: {var.shape}, Size: {var.size}")

        # Determine max string length by processing in chunks
        max_length = 0

        if isinstance(var.data, da.Array):
            # Dask array - process chunk by chunk
            logger.info(
                f"  Finding max length for var: {var_name} using Dask chunks..."
            )

            # Process each chunk to find max length
            for chunk_idx in range(
                var.data.npartitions if hasattr(var.data, "npartitions") else 1
            ):
                try:
                    # Get chunk and compute it
                    chunk = (
                        var.data.blocks[chunk_idx]
                        if hasattr(var.data, "blocks")
                        else var.data
                    )
                    chunk_data = chunk.compute()

                    # Find max length in this chunk
                    chunk_lengths = [
                        len(str(item)) if item is not None else 0
                        for item in chunk_data.flat
                    ]
                    chunk_max = max(chunk_lengths) if chunk_lengths else 0
                    max_length = max(max_length, chunk_max)

                except Exception as e:
                    logger.warning(f"    Error processing chunk {chunk_idx}: {e}")
                    # If we can't process chunks individually, fall back to computing all
                    break

            # If chunk processing failed or max_length is still 0, compute the whole array
            if max_length == 0:
                logger.info(f"  Computing entire array to find max length...")
                computed_data = var.compute()
                all_lengths = [
                    len(str(item)) if item is not None else 0
                    for item in computed_data.values.flat
                ]
                max_length = max(all_lengths) if all_lengths else 0
        else:
            # Already a numpy array - process directly
            logger.info(f"  Processing numpy array...")
            all_lengths = [
                len(str(item)) if item is not None else 0 for item in var.values.flat
            ]
            max_length = max(all_lengths) if all_lengths else 0

        # Add buffer to max_length (20% extra or at least 16 bytes)
        safe_length = max(16, int(max_length * 1.2))

        # Choose appropriate dtype based on length
        if safe_length <= 32:
            dtype = "S32"
        elif safe_length <= 64:
            dtype = "S64"
        elif safe_length <= 128:
            dtype = "S128"
        elif safe_length <= 256:
            dtype = "S256"
        else:
            logger.warning(
                f"  Very long strings detected in variable {var_name} (length: {safe_length}). "
                f"Using dtype S{safe_length}, which may increase file size."
            )
            dtype = f"S{safe_length}"

        logger.info(f"  Max length found: {max_length}, using dtype: {dtype}")

        # Convert the variable to fixed-size string
        dataset[var_name] = var.astype(dtype)
        logger.info(f"  Converted {var_name} to {dtype}")

    return dataset


def ignore_invalid_unicode_in_attrs(dataset: xarray.Dataset) -> xarray.Dataset:
    for k, v in dataset.attrs.items():
        if isinstance(v, str):
            dataset.attrs[k] = v.encode("utf-8", errors="ignore").decode("utf-8")

    return dataset


def is_dim(key: str, dataset: xarray.Dataset) -> bool:
    return key in dataset.dims


def is_var(key: str, dataset: xarray.Dataset) -> bool:
    # both coords and data_vars are in variables
    return key in dataset.variables


def form_mask(
    existing_mask: DataArray | None,
    key: str,
    min_value: any,
    max_value: any,
    ds: xarray.Dataset,
) -> any:
    var_mask = (ds[key] >= min_value) & (ds[key] <= max_value)
    if existing_mask is None:
        return var_mask
    else:
        return existing_mask & var_mask


def form_dim_conditions(
    existing_conditions: dict[str, slice] | None,
    key: str,
    min_value: any,
    max_value: any,
    dataset: xarray.Dataset,
) -> dict[str, slice]:
    # try to know the dim is ascending or descending
    slice_from = min_value
    slice_to = max_value

    # if descending, swap
    if dataset[key][0] > dataset[key][-1]:
        slice_from = max_value
        slice_to = min_value

    dim_condition = {key: slice(slice_from, slice_to)}
    if existing_conditions is None:
        return dim_condition
    else:
        existing_conditions.update(dim_condition)
        return existing_conditions
