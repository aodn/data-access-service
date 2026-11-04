"""Download for a collection with no cloud-optimised data.

The user gets a list of the collection's data-index files that match the
request (a zipped CSV, one row and one link per file), not the files themselves.
The collection has one key here: ORIGINAL_NETCDF_KEY.
"""

import csv
import io
import tempfile
import zipfile
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import quote

import pandas as pd

from data_access_service import init_log, Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.duckdbclients.dataindexclients import (
    DataIndexDuckDBClient,
)
from data_access_service.core.data_index_files import (
    ORIGINAL_NETCDF_KEY,
    find_data_index_files,
)
from data_access_service.models.data_index_types import DataIndexFile
from data_access_service.models.subset_request import NON_SPECIFIED, SubsetRequest
from data_access_service.utils.date_time_utils import (
    resolve_non_specified_dates,
    to_utc_bounds,
)
from data_access_service.utils.email_templates.download_email import (
    get_download_email_html_body,
)
from data_access_service.utils.email_templates.no_data_email import (
    NO_DATA_EMAIL_SUBJECT,
    get_no_data_email_html_body,
)

FILE_LIST_COLUMNS = [
    "url",
    "key",
    "time_coverage_start",
    "time_coverage_end",
    "geospatial_lat_min",
    "geospatial_lat_max",
    "geospatial_lon_min",
    "geospatial_lon_max",
]


def _iso(value: Optional[datetime]) -> str:
    """A CSV cell for a time; empty when the index has no time for the file."""
    return value.isoformat() if value is not None else ""


class DataIndexProcessor:
    def __init__(self, job_id: str, subset_request: SubsetRequest):
        self.aws = AWSHelper()
        self.config = Config.get_config()
        self.log = init_log(self.config)
        self.job_id = job_id
        self.subset_request = subset_request
        # The indexed files sit in the same region as the index.
        self.region = self.config.get_data_index_config().region

    @property
    def uuid(self) -> str:
        return self.subset_request.uuid

    @property
    def recipient(self) -> str:
        return self.subset_request.recipient

    def process(self) -> None:
        self.log.info(
            f"Start data-index download for uuid: {self.uuid}, job id: {self.job_id}"
        )
        keys = self.subset_request.keys
        if "*" in keys or ORIGINAL_NETCDF_KEY in keys:
            # The email shows the actual key, as on the cloud-optimised path.
            # One key, so the file name has no key in it.
            self.subset_request = replace(
                self.subset_request,
                keys=[ORIGINAL_NETCDF_KEY],
                collection_has_multi_datasets=False,
            )
            files = self.__find_files()
        else:
            self.log.warning(
                "uuid %s: unknown keys %s, the only key here is %s",
                self.uuid,
                keys,
                ORIGINAL_NETCDF_KEY,
            )
            files = []

        if not files:
            self.aws.send_email(
                recipient=self.recipient,
                subject=NO_DATA_EMAIL_SUBJECT,
                html_body=get_no_data_email_html_body(self.subset_request),
            )
            return

        url = self.__upload_file_list(files)
        self.aws.send_email(
            recipient=self.recipient,
            subject=f"Finish processing data file whose uuid is:  {self.uuid}",
            html_body=get_download_email_html_body(
                subset_request=self.subset_request, object_urls=[url]
            ),
        )

    def __time_range(
        self,
    ) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        """The request's UTC bounds; None for a side left "non-specified".

        No bound on that side, so no file is dropped there: the default
        1970..today would drop older files (e.g. NRS data from 1944).
        """
        start_date = self.subset_request.start_date
        end_date = self.subset_request.end_date
        # to_utc_bounds needs both sides, so an open side is filled, then dropped.
        start, end = to_utc_bounds(*resolve_non_specified_dates(start_date, end_date))
        return (
            None if start_date == NON_SPECIFIED else start,
            None if end_date == NON_SPECIFIED else end,
        )

    def __find_files(self) -> List[DataIndexFile]:
        start, end = self.__time_range()
        with DataIndexDuckDBClient() as client:
            files = find_data_index_files(
                client,
                uuid=self.uuid,
                start=start,
                end=end,
                bboxes=self.subset_request.bboxes,
            )
        self.log.info("Found %d data-index files for uuid %s", len(files), self.uuid)
        return files

    def __file_url(self, file: DataIndexFile) -> str:
        return f"https://{file.bucket}.s3.{self.region}.amazonaws.com/{quote(file.key)}"

    def __upload_file_list(self, files: List[DataIndexFile]) -> str:
        """Write the file list as a zipped CSV, upload it, return its link."""
        # Named after the collection title, or the uuid when there is no title.
        base_name = f"{self.subset_request.download_base_name(self.uuid)}-file-list"
        bucket_name = self.config.get_subsetting_bucket_name()
        s3_key = f"{self.job_id}/{base_name}.zip"

        with tempfile.TemporaryDirectory() as work_dir:
            zip_path = Path(work_dir) / f"{base_name}.zip"
            # Write the CSV straight into the zip, so a big list is not held
            # as one string or written to disk twice.
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                with zf.open(f"{base_name}.csv", "w") as raw, io.TextIOWrapper(
                    raw, encoding="utf-8", newline=""
                ) as text:
                    writer = csv.writer(text)
                    writer.writerow(FILE_LIST_COLUMNS)
                    for file in files:
                        writer.writerow(
                            [
                                self.__file_url(file),
                                file.key,
                                _iso(file.time_coverage_start),
                                _iso(file.time_coverage_end),
                                file.lat_min,
                                file.lat_max,
                                file.lon_min,
                                file.lon_max,
                            ]
                        )

            self.log.info(
                "Uploading file list to s3 bucket: %s, key: %s", bucket_name, s3_key
            )
            return self.aws.upload_file_to_s3(str(zip_path), bucket_name, s3_key)
