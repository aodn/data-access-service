import math
import os
import tempfile
from pathlib import Path
from typing import List

import dask
import numcodecs
import psutil
import xarray
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

        self.log.info("Processing keys: %s", self.keys)
        for key in self.keys:
            self.log.info("Processing key: %s", key)

            dataset = self.__get_zarr_dataset_for_(key=key)
            print("already got dataset for key", key)
            if dataset is None:
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

            print("easily got zarr store for key", key)
            print(
                "memory usage here:",
                psutil.Process(os.getpid()).memory_info().rss / (1024**3),
                "GB",
            )

            # Ensure the dataset is dask-backed (lazy loading)
            # Check if dataset is dask-backed WITHOUT loading data into memory
            # import dask.array as da
            # if not any(
            #     isinstance(var.data, da.Array)
            #     for var in dataset.data_vars.values()
            # ):
            #     self.log.warning(
            #         f"Dataset {key} is not dask-backed, rechunking with 'auto'"
            #     )
            #     dataset = dataset.chunk("auto")
            # print("dataset after chunking", dataset)
            conditions = self.get_all_subset_conditions(key, bbox)

            time_dim = self.api.map_column_names(
                uuid=self.uuid, key=key, columns=["TIME"]
            )[0]
            time_per_chunk = self.__get_time_steps_per_chunk(dataset, time_dim)
            self.log.info(
                "Chunking dataset with %d time steps per chunk", time_per_chunk
            )
            dataset = dataset.chunk({time_dim: time_per_chunk})
            dim_conditions = {}  # Conditions for dimensions of zarr
            mask = None  # Conditions for variables (include coord and data_var) of zarr. Naming "mask" is for zarr convention
            for k, val_range in conditions.items():
                print("forming condition for key", k, "with range", val_range)
                if is_dim(key=k, dataset=dataset):
                    dim_conditions[k] = slice(val_range[0], val_range[1])
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

            print(
                "current memory usage before applying conditions:",
                psutil.Process(os.getpid()).memory_info().rss / (1024**3),
                "GB",
            )
            # apply dim conditions and mask (variable conditions)
            print("Applying conditions for key", key)
            subset = dataset
            if dim_conditions:
                subset = subset.sel(**dim_conditions)
            if mask is not None:

                # please use drop=False to keep lazy loading
                subset = subset.where(mask, drop=False)
                print("subset after where", subset)

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
        print("final merged dataset for key", key, "is", merged_dataset)
        return merged_dataset

    def __write_to_s3_as_netcdf(self, dataset: xarray.Dataset, key: str):
        dataset = ignore_invalid_unicode_in_attrs(dataset)
        time_dim = self.api.map_column_names(uuid=self.uuid, key=key, columns=["TIME"])[
            0
        ]
        time_per_chunk = self.__get_time_steps_per_chunk(dataset, time_dim)
        self.log.info("Chunking dataset with %d time steps per chunk", time_per_chunk)
        dataset = dataset.chunk({time_dim: time_per_chunk})

        zarr_compression = {
            var: {"compressor": numcodecs.Blosc(cname="zstd", clevel=5)}
            for var in dataset.data_vars
        }

        netcdf_compression = {
            var: {"zlib": True, "complevel": 5}
            for var, da in dataset.data_vars.items()
            if da.dtype.kind in {"i", "u", "f"}  # integer, unsigned, float
        }
        thread_count = self.get_available_thread_count()

        # set the thread count for dask, for the to_netcdf operation later
        dask.config.set(num_workers=thread_count)
        bucket_name = self.config.get_csv_bucket_name()

        with tempfile.TemporaryDirectory() as tempdirname:

            with ProcessLogger(logger=self.log, task_name="Writing to s3 as netcdf"):
                temp_netcdf_path = Path(tempdirname) / key.replace(".zarr", ".nc")
                dataset.to_netcdf(
                    temp_netcdf_path,
                    engine="netcdf4",
                    encoding=netcdf_compression,
                    compute=True,
                )
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
        self, dataset: xarray.Dataset, time_dim: str, memory_fraction: float = 0.3
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
            "Chunk size: %d GB per thread", safe_memory_per_thread / (1024**3)
        )

        # CRITICAL FIX: Use metadata to estimate size WITHOUT forcing computation
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

        print("tz-aware?", self.start_date.tzinfo, self.end_date.tzinfo)

        return {
            time_dim: [
                self.start_date.tz_convert("UTC").tz_localize(None),
                self.end_date.tz_convert("UTC").tz_localize(None),
            ],
            lat_dim: [bbox.min_lat, bbox.max_lat],
            lon_dim: [bbox.min_lon, bbox.max_lon],
        }


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
    print("forming mask for key", key, "with min", min_value, "and max", max_value)
    var_mask = (ds[key] >= min_value) & (ds[key] <= max_value)
    print("var_mask for key", key, "is", var_mask)
    if existing_mask is None:
        print("returning new mask for key", key)
        return var_mask
    else:
        print("combining with existing mask for key", key)
        return existing_mask & var_mask
