import math
import os
import tempfile
from pathlib import Path
from typing import List

import gc
import time
import dask
import psutil
import xarray

import numpy as np
import dask.array as da
from xarray import DataArray

from data_access_service import init_log, Config, API
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.constants import (
    STR_TIME_UPPER_CASE,
    STR_LATITUDE_UPPER_CASE,
    STR_LONGITUDE_UPPER_CASE,
    WHOLE_GLOBE_BBOX,
)
from data_access_service.utils.subsetting_resolver import (
    ResolvedSubset,
    resolve_subset,
)
from data_access_service.utils.date_time_utils import to_naive_utc
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils import geotiff_export
from data_access_service.utils.process_logger import ProcessLogger


class ZarrProcessor:
    def __init__(
        self,
        api: API,
        job_id: str,
        subset_request: SubsetRequest,
        resolved: ResolvedSubset | None = None,
    ):
        self.aws = AWSHelper()
        self.api = api
        self.config = Config.get_config()
        self.log = init_log(self.config)
        self.job_id = job_id
        self.subset_request = subset_request

        if resolved is None:
            resolved = resolve_subset(
                api=api,
                uuid=subset_request.uuid,
                keys=subset_request.keys,
                start_date_str=subset_request.start_date,
                end_date_str=subset_request.end_date,
                multi_polygon=subset_request.multi_polygon,
            )
        if not resolved.has_data:
            # The caller (batch init) checks has_data and sends the "no data"
            # email before constructing a processor; getting here is a bug.
            raise ValueError(
                f"Requested date range {subset_request.start_date}..{subset_request.end_date} "
                f"has no data for dataset {subset_request.uuid}, keys {resolved.keys}"
            )

        self.keys = resolved.keys
        self.start_date = resolved.start_date
        self.end_date = resolved.end_date
        # empty bboxes means "no spatial filter"; the slicing needs explicit bounds
        self.bboxes = resolved.bboxes or [WHOLE_GLOBE_BBOX]

    # Read-through views onto subset_request — single source of truth, no drift.
    @property
    def uuid(self) -> str:
        return self.subset_request.uuid

    @property
    def recipient(self) -> str:
        return self.subset_request.recipient

    @property
    def output_format(self) -> str:
        return self.subset_request.output_format

    def process(self):
        self.log.info(
            f"Start processing zarr data for uuid: {self.uuid}, job id: {self.job_id}"
        )
        urls: List[str] = []

        prepare, write = self.__format_handler()

        self.log.info("Datasets to process: %s", self.keys)
        for key in self.keys:
            self.log.info("Processing dataset: %s", key)
            dataset = self.__get_zarr_dataset_for_(key=key, prepare=prepare)
            if dataset is None:
                self.log.info("No data found for dataset: %s, skipping...", key)
                continue

            download_uri = write(dataset=dataset, key=key)
            urls.extend(download_uri)
        subject = f"Finish processing data file whose uuid is:  {self.uuid}"

        html_content = get_download_email_html_body(
            subset_request=self.subset_request, object_urls=urls
        )

        self.aws.send_email(
            recipient=self.recipient, subject=subject, html_body=html_content
        )

    def __format_handler(self):
        """Return the (prepare, write) pair for the requested output format.

        Each format is self-contained: prepare does any format-specific tweak to
        the dataset before subsetting, write saves it and returns download URLs.
        Add a new format by adding one entry here - no other code changes.
        """
        handlers = {
            "netcdf": (self.__prepare_netcdf, self.__write_to_s3_as_netcdf),
            "geotiff": (self.__prepare_geotiff, self.__write_to_s3_as_geotiff),
        }
        if self.output_format not in handlers:
            raise ValueError(f"Unsupported format: {self.output_format}")
        return handlers[self.output_format]

    def __prepare_netcdf(self, zarr_store, key):
        """NetCDF keeps the dataset as-is (2D coords are written losslessly)."""
        return zarr_store

    def __prepare_geotiff(self, zarr_store, key):
        """GeoTIFF needs 1D axes, so reduce a regular grid (curvilinear stays 2D)."""
        if not geotiff_export.has_ij_dims(zarr_store):
            return zarr_store
        lat_name, lon_name, _ = self.__get_dim_names(key)
        return geotiff_export.prepare_grid_for_geotiff(
            zarr_store, lat_name, lon_name, self.log
        )

    def __get_zarr_dataset_for_(self, key: str, prepare) -> xarray.Dataset | None:

        zarr_store = self.api._instance.get_dataset(key).zarr_store
        zarr_store = prepare(zarr_store, key)  # format-specific prep, no `if format`

        # apply subsetting conditions for each bbox and merge them
        merged_dataset: xarray.Dataset | None = None
        for bbox in self.bboxes:

            dataset = zarr_store

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
                self.log.info(
                    "forming condition for key %s with range %s", k, val_range
                )
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
        bucket_name = self.config.get_subsetting_bucket_name()

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
            return [f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"]

    def __write_to_s3_as_geotiff(self, dataset: xarray.Dataset, key: str) -> List[str]:
        """Build the GeoTIFF ZIP for the dataset and upload it to S3."""
        lat_name, lon_name, time_name = self.__get_dim_names(key)
        dataset_base = key.replace(".zarr", "")
        zip_name = f"{dataset_base}_geotiff.zip"

        with tempfile.TemporaryDirectory() as work_dir:
            zip_path = Path(work_dir) / zip_name
            geotiff_export.build_geotiff_zip(
                dataset, zip_path, dataset_base, lat_name, lon_name, time_name, self.log
            )
            url = self.__upload_zip_to_s3(zip_path, zip_name)

        self.log.info(f"Exported GeoTIFF ZIP for {key}")
        return [url]

    def __get_dim_names(self, key: str):
        """Resolve the actual lat, lon, and time dimension names for a dataset."""
        lat_name = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_LATITUDE_UPPER_CASE]
        )[0]
        lon_name = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_LONGITUDE_UPPER_CASE]
        )[0]
        time_name = self.api.map_column_names(
            uuid=self.uuid, key=key, columns=[STR_TIME_UPPER_CASE]
        )[0]
        return lat_name, lon_name, time_name

    def __upload_zip_to_s3(self, zip_path: Path, zip_name: str) -> str:
        """Upload a ZIP file to S3 and return the download URL."""
        bucket_name = self.config.get_subsetting_bucket_name()
        s3_key = f"{self.job_id}/{zip_name}"
        self.aws.upload_file_to_s3(str(zip_path), bucket_name, s3_key)
        region = self.aws.s3.meta.region_name
        url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"
        self.log.info(f"Uploaded: {s3_key}")
        zip_path.unlink(missing_ok=True)
        return url

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
                to_naive_utc(self.start_date),
                to_naive_utc(self.end_date),
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
