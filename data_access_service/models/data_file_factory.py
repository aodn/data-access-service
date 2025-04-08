import datetime
import sys
from typing import Optional

import pandas as pd


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
        return sys.getsizeof(self) > 512 * 1024 * 1024  # 512MB

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

        csv_file_path = (f"{folder_name}/date:{self.start_date}~{self.end_date}|"
                         f"bbox:{self.min_lon},{self.min_lat}, {self.max_lon}, {self.max_lat} .csv")
        self.data_frame.to_csv(csv_file_path, index=False)

        # clean the data_frame
        self.data_frame = None
        self.log.info(f"Data converted into csv: {csv_file_path}")
        self.log.info(f"conditions: {self.start_date}~{self.end_date}|"
                      f"bbox:{self.min_lon},{self.min_lat}, {self.max_lon}, {self.max_lat}")

        return csv_file_path
