import math
import os
import tempfile
from pathlib import Path
from typing import List

import dask
import numcodecs
import psutil
import xarray

from data_access_service import init_log, Config, API
from data_access_service.batch.subsetting_helper import trim_date_range_for_keys
from data_access_service.core.AWSHelper import AWSHelper
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

        for key in self.keys:

            dataset = self.__get_zarr_dataset_for_(key=key)
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
        merged_dataset: xarray.Dataset | None = None
        for bbox in self.bboxes:

            subset = self.api.get_dataset(
                uuid=self.uuid,
                key=key,
                date_start=self.start_date,
                date_end=self.end_date,
                lat_min=bbox.min_lat,
                lat_max=bbox.max_lat,
                lon_min=bbox.min_lon,
                lon_max=bbox.max_lon,
            )
            if subset is None:
                continue

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
            self.log.info("Running in dev or testing mode, using 2 threads")
            return 2

        cpu_count = psutil.cpu_count(logical=True)
        self.log.info(f"Available thread count: {cpu_count}")
        return cpu_count

    def __get_time_steps_per_chunk(
        self, dataset: xarray.Dataset, time_dim: str, memory_fraction: float = 0.5
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
        total_size = sum(var.nbytes for var in dataset.values())
        chunk_count = max(1, math.ceil(total_size / safe_memory_per_thread))
        self.log.info("Chunk count: %d", chunk_count)
        total_time_count = dataset.sizes[time_dim]
        return math.ceil(total_time_count / chunk_count)


def ignore_invalid_unicode_in_attrs(dataset: xarray.Dataset) -> xarray.Dataset:
    for k, v in dataset.attrs.items():
        if isinstance(v, str):
            dataset.attrs[k] = v.encode("utf-8", errors="ignore").decode("utf-8")

    return dataset
