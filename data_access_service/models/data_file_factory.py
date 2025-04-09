import datetime
import os
import sys
from typing import Optional

import pandas as pd

from data_access_service.utils.date_time_utils import YEAR_MONTH_DAY


class DataFileFactory:


    # TODO: should use polygon to instead bbox later when cloud optimized library is upgraded
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    data_frame: Optional[pd.DataFrame]
    start_date: Optional[datetime]
    end_date: Optional[datetime]

    def __init__(self, min_lat, max_lat, min_lon, max_lon, log):
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.start_date = None
        self.end_date = None
        self.data_frame = None
        self.log = log

    def is_full(self):
        size = sys.getsizeof(self.data_frame)
        sizeInMb = size / (1024 * 1024)
        self.log.info(f"DataFrame size: {sizeInMb:.2f} MB")
        self.log.info(sys.getsizeof(self.data_frame) > 512 * 1024 * 1024)
        return sys.getsizeof(self.data_frame) > 512 * 1024 * 1024  # 512MB

    def add_data(self, data_frame, start_date, end_date):
        if self.data_frame is None:
            self.data_frame = data_frame
        else:
            self.data_frame = pd.concat([self.data_frame, data_frame], ignore_index=True)

        if self.start_date is None or start_date < self.start_date:
            self.start_date = start_date
        if self.end_date is None or end_date > self.end_date:
            self.end_date = end_date


    def save_as_csv_in_folder_(self, folder_name: str) -> str:
        if self.data_frame is None or self.data_frame.empty:
            raise ValueError("No data found to convert to CSV")

        # if this folder does not exist, create it
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        csv_file_path = (f"{folder_name}/date_{self.start_date.strftime(YEAR_MONTH_DAY)}"
                         f"_{self.end_date.strftime(YEAR_MONTH_DAY)}_"
                         f"bbox_{int(round(self.min_lon, 0))}_{int(round(self.min_lat, 0))}"
                         f"_{int(round(self.max_lon, 0))}_{int(round(self.max_lat, 0))}.csv")

        self.data_frame.to_csv(csv_file_path, index=False)

        # cleanup
        self.data_frame = None
        self.start_date = None
        self.end_date = None
        self.log.info(f"Data converted into csv: {csv_file_path}")
        self.log.info(f"conditions: {self.start_date}~{self.end_date}|"
                      f"bbox:{self.min_lon},{self.min_lat}, {self.max_lon}, {self.max_lat}")
        self.log.info(f"current size of data frame: {sys.getsizeof(self.data_frame)}")

        return csv_file_path
