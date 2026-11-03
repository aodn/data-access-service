"""Download for a collection with no cloud-optimised data.

The user gets a list of the collection's original NetCDF files that match the
request (a zipped CSV, one row and one link per file), not the files themselves.
"""

import csv
import io
import tempfile
import zipfile
from pathlib import Path
from typing import List
from urllib.parse import quote

from data_access_service import init_log, Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.duckdbclients.dataindexclients import (
    DataIndexDuckDBClient,
)
from data_access_service.core.original_netcdf_index import find_original_netcdfs
from data_access_service.models.data_index_types import OriginalNetcdf
from data_access_service.models.subset_request import SubsetRequest
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


class OriginalNetcdfProcessor:
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
            f"Start original NetCDF download for uuid: {self.uuid}, job id: {self.job_id}"
        )
        start, end = to_utc_bounds(
            *resolve_non_specified_dates(
                self.subset_request.start_date, self.subset_request.end_date
            )
        )
        with DataIndexDuckDBClient() as client:
            files = find_original_netcdfs(
                client,
                uuid=self.uuid,
                start=start,
                end=end,
                bboxes=self.subset_request.bboxes,
            )
        self.log.info(
            "Found %d original NetCDF files for uuid %s", len(files), self.uuid
        )

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

    def __file_url(self, file: OriginalNetcdf) -> str:
        return f"https://{file.bucket}.s3.{self.region}.amazonaws.com/{quote(file.key)}"

    def __upload_file_list(self, files: List[OriginalNetcdf]) -> str:
        """Write the file list as a zipped CSV, upload it, return its link."""
        # Named after the collection title, or the uuid when there is no title.
        # There is no dataset key on this path.
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
                                file.time_coverage_start.isoformat(),
                                file.time_coverage_end.isoformat(),
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
