import math
import tempfile
from typing import List

import dask
import psutil
import xarray

from data_access_service import init_log, Config
from data_access_service.batch.subsetting_helper import trim_date_range_for_keys
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.models.multi_polygon_helper import MultiPolygonHelper
from data_access_service.server import api_setup, app
from data_access_service.utils.date_time_utils import supply_day_with_nano_precision

CHUNK_SIZE = 2 * 1024 * 1024 * 1024  # 2GB


class ZarrProcessor:
    def __init__(
        self,
        uuid: str,
        job_id: str,
        keys: List[str],
        start_date_str: str,
        end_date_str: str,
        multi_polygon: str,
        recipient: str,
    ):
        self.aws = AWSHelper()
        self.api = api_setup(app)
        self.config = Config.get_config()
        self.log = init_log(self.config)
        self.uuid = uuid
        self.job_id = job_id
        self.recipient = recipient
        start_date, end_date = supply_day_with_nano_precision(
            start_date_str=start_date_str, end_date_str=end_date_str
        )

        if "*" in keys:
            md = self.api.get_mapped_meta_data(self.uuid)
            self.keys = list(md.keys())

        trimmed_start_date, trimmed_end_date = trim_date_range_for_keys(
            uuid=uuid,
            keys=self.keys,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
        self.start_date = trimmed_start_date
        self.end_date = trimmed_end_date
        self.multi_polygon = MultiPolygonHelper(multi_polygon=multi_polygon)
        self.api = api_setup(app)

    def process(self):
        self.log(
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
        self.aws.send_email(
            recipient=self.recipient, subject=subject, download_urls=urls
        )

    def __get_zarr_dataset_for_(self, key: str) -> xarray.Dataset | None:
        merged_dataset: xarray.Dataset | None = None
        for bbox in self.multi_polygon.bboxes:

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
                merged_dataset = xarray.concat([merged_dataset, subset])

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
        time_per_chunk = self.__get_time_count_per_chunk(key=key, dataset=dataset)
        dataset.chunk({time_dim: time_per_chunk})

        compression = {
            var: {"zlib": True, "complevel": 5}
            for var, da in dataset.data_vars.items()
            if da.dtype.kind in {"i", "u", "f"}  # integer, unsigned, float
        }
        thread_count = self.get_available_thread_count()
        dask.config.set(num_workers=thread_count)
        bucket_name = self.config.get_csv_bucket_name()
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=True) as temp_file:

            dataset.to_netcdf(temp_file.name, engine="netcdf4", encoding=compression)
            s3_key = f"{self.job_id}/{key.replace('.zarr', '.nc')}"
            self.aws.upload_file_to_s3(temp_file.name, bucket_name, s3_key)
            region = self.aws.s3.meta.region_name

            return f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

    def __get_time_count_per_chunk(self, key: str, dataset: xarray.Dataset) -> int:
        total_size = sum(var.nbytes for var in dataset.values())
        chunk_count = math.ceil(total_size / CHUNK_SIZE)
        time_dim = self.api.map_column_names(uuid=self.uuid, key=key, columns=["TIME"])[
            0
        ]
        total_time_count = dataset.sizes[time_dim]
        return math.ceil(total_time_count / chunk_count)

    def get_available_thread_count(self):
        cpu_count = psutil.cpu_count(logical=True)
        self.log.info(f"Available thread count: {cpu_count}")
        return cpu_count


def ignore_invalid_unicode_in_attrs(dataset: xarray.Dataset):
    for k, v in dataset.attrs.items():
        if isinstance(v, str):
            dataset.attrs[k] = v.encode("utf-8", errors="ignore").decode("utf-8")

    return dataset
