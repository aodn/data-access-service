import logging
from typing import List

import psutil
import xarray

from data_access_service.batch.subsetting_helper import trim_date_range_for_keys
from data_access_service.models.multi_polygon_helper import MultiPolygonHelper
from data_access_service.server import api_setup, app
from data_access_service.utils.date_time_utils import supply_day_with_nano_precision

CHUNK_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

log = init_log(config)


class ZarrProcessor:
    def __init__(
        self,
        uuid: str,
        keys: List[str],
        start_date_str: str,
        end_date_str: str,
        multi_polygon: str,
    ):
        self.api = api_setup(app)
        self.uuid = uuid
        self.keys = keys
        start_date, end_date = supply_day_with_nano_precision(
            start_date_str=start_date_str, end_date_str=end_date_str
        )
        trimmed_start_date, trimmed_end_date = trim_date_range_for_keys(
            uuid=uuid,
            keys=keys,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
        self.start_date = trimmed_start_date
        self.end_date = trimmed_end_date
        self.multi_polygon = MultiPolygonHelper(multi_polygon=multi_polygon)
        self.api = api_setup(app)

    def process(self):
        for key in self.keys:
            dataset = self.__get_zarr_dataset_for_(key=key)

    def __get_zarr_dataset_for_(self, key: str):
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

            if merged_dataset is None:
                merged_dataset = subset
            else:
                merged_dataset = xarray.concat([merged_dataset, subset])

        return merged_dataset


def get_available_thread_count():
    cpu_info = psutil.cpu_count(logical=True)
    log.info(f"Available thread count: {cpu_info}")
    return cpu_info
