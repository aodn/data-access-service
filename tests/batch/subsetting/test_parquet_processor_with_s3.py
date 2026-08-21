import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from aodn_cloud_optimised.lib import DataQuery
from aodn_cloud_optimised.lib.DataQuery import Metadata

from data_access_service import API
from data_access_service.batch.subsetting import prepare_data
from data_access_service.config.config import Config
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.core.constants import STR_TIME_UPPER_CASE
from data_access_service.utils.date_time_utils import parse_date
from tests.core.test_with_s3 import TestWithS3, REGION

SEAGRASS_KEY = "aggregated_seagrass_nonqc.parquet"
SEAGRASS_UUID = "009a1131-efc1-4a61-8f90-cf289e7c043d"
MASTER_JOB_ID = "542e76c3-6c19-463e-8b85-0b62db45f045"
# Canned seagrass rows fall on 2025-02-23, which is the last date_ranges slot.
SEAGRASS_JOB_INDEX = "55"


def _canned_s3_sample2() -> Path:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "canned" / "s3_sample2"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("canned/s3_sample2 not found")


SEAGRASS_PREPARATION_PARAMETERS = {
    "end_date": "non-specified",
    "date_ranges": json.dumps(
        {
            str(i): [
                f"{year}-01-01 00:00:00.000000000",
                (
                    "2025-02-23 23:59:59.999999999"
                    if year == 2025
                    else f"{year}-12-31 23:59:59.999999999"
                ),
            ]
            for i, year in enumerate(range(1970, 2026))
        }
    ),
    "master_job_id": MASTER_JOB_ID,
    "full_metadata_link": (
        "https://portal-edge.aodn.org.au/details/"
        "009a1131-efc1-4a61-8f90-cf289e7c043d"
    ),
    "type": "sub-setting-data-preparation",
    "uuid": SEAGRASS_UUID,
    "intermediate_output_folder": f"/tmp/tmpdocp2iqj{MASTER_JOB_ID}",
    "collection_title": (
        "Australian Seagrass Occurence - Aggregated data product "
        "(1967 - ongoing) (NESP MaC 5.9, IMOS)"
    ),
    "suggested_citation": (
        'The citation in a list of references is: "IMOS [year-of-data-download], '
        "Australian Seagrass Occurence - Aggregated data product "
        "(1967 - ongoing) (NESP MaC 5.9, IMOS), [data-access-URL], "
        'accessed [date-of-access]."'
    ),
    "output_format": "csv",
    # No spatial filter: canned seagrass hive `polygon` values do not match the
    # WKB hex create_bbox_filter compares against, so any MultiPolygon yields
    # an empty table. This test is about the time window.
    "multi_polygon": None,
    "recipient": "manfai.ng@utas.edu.au",
    "key": SEAGRASS_KEY,
    "start_date": "non-specified",
}


class TestParquetProcessorWithS3(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            _canned_s3_sample2(),
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_parquet_processor_with_s3(
        self,
        aws_clients,
        setup_resources,
        upload_test_case_to_s3,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()
        config.set_s3_client(s3_client)

        aodn = DataQuery.GetAodn()
        metadata: Metadata = aodn.get_metadata()
        assert metadata.metadata_catalog().get(SEAGRASS_KEY) is not None

        api = API()
        api.initialize_metadata()

        with patch.object(AWSHelper, "send_email"):
            try:
                prepare_data(
                    api,
                    job_index=SEAGRASS_JOB_INDEX,
                    parameters=SEAGRASS_PREPARATION_PARAMETERS,
                )

                bucket_name = config.get_subsetting_bucket_name()
                response = s3_client.list_objects_v2(Bucket=bucket_name)
                objects = (
                    [obj["Key"] for obj in response["Contents"]]
                    if "Contents" in response
                    else []
                )
                assert (
                    f"{MASTER_JOB_ID}/temp/dataschema.json" in objects
                ), f"prepare_data did not upload a schema for {SEAGRASS_KEY}: {objects}"
                parquet_objects = [key for key in objects if key.endswith(".parquet")]
                assert (
                    parquet_objects
                ), f"prepare_data did not upload subset parquet for {SEAGRASS_KEY}: {objects}"

                helper = AWSHelper()
                subset_path = (
                    f"s3://{bucket_name}/"
                    f"{config.get_s3_temp_folder_name(MASTER_JOB_ID)}"
                    f"{SEAGRASS_KEY}"
                )
                subset = helper.read_parquet_from_s3(subset_path)
                assert len(subset) > 0, "subset parquet is empty"

                time_key = api.map_column_names(
                    uuid=SEAGRASS_UUID,
                    key=SEAGRASS_KEY,
                    columns=[STR_TIME_UPPER_CASE],
                )[0]
                date_ranges = json.loads(SEAGRASS_PREPARATION_PARAMETERS["date_ranges"])
                start_date = parse_date(date_ranges[SEAGRASS_JOB_INDEX][0])
                end_date = parse_date(date_ranges[SEAGRASS_JOB_INDEX][1])

                times = pd.to_datetime(subset[time_key].compute())
                if times.dt.tz is None:
                    times = times.dt.tz_localize("UTC")
                else:
                    times = times.dt.tz_convert("UTC")

                assert (
                    times.min() >= start_date
                ), f"subset time {times.min()} is before range start {start_date}"
                assert (
                    times.max() <= end_date
                ), f"subset time {times.max()} is after range end {end_date}"
            finally:
                shutil.rmtree(config.get_temp_folder(MASTER_JOB_ID), ignore_errors=True)
                shutil.rmtree(
                    SEAGRASS_PREPARATION_PARAMETERS["intermediate_output_folder"],
                    ignore_errors=True,
                )
