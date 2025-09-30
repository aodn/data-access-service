import json
from io import BytesIO

from data_access_service import Config, init_log
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.server import api_setup, app
from data_access_service.batch.batch_enums import Parameters
from data_access_service.utils.date_time_utils import (
    get_monthly_utc_date_range_array_from_,
)
from data_access_service.utils.routes_helper import fetch_data, async_response_json


def init_co_indexing_data():
    aws = AWSHelper()
    api = api_setup(app)
    config = Config.get_config()
    metadata_dict = api.get_mapped_meta_data(None)
    subjob_params = []
    for uuid, value in metadata_dict.items():
        for key in value:
            # don't need to prepare zarr for now
            if key.endswith(".zarr"):
                continue

            subjob_params.append({"uuid": uuid, "key": key})

    print(subjob_params)
    indexing_preparation_job_id = aws.submit_a_job(
        job_name="co-indexing-data-preparation",
        job_queue=config.get_job_queue_name(),
        job_definition=config.get_job_definition_name(),
        parameters={
            Parameters.TYPE.value: "cloud-optimised-data-index-preparation",
            Parameters.INDEX_DATASETS.value: json.dumps(subjob_params),
        },
        array_size=len(subjob_params),
    )


def prepare_co_indexing_data(job_index, parameters):
    aws = AWSHelper()
    config = Config.get_config()
    api = api_setup(app)
    log = init_log(config)

    if job_index is None:
        raise ValueError("Job index is required for co-indexing data preparation")

    index_datasets = json.loads(parameters[Parameters.INDEX_DATASETS.value])
    if int(job_index) >= len(index_datasets):
        raise ValueError("Job index is out of range for co-indexing data preparation")

    dataset = index_datasets[int(job_index)]
    uuid = dataset["uuid"]
    key = dataset["key"]

    start_date, end_date = api.get_temporal_extent(uuid, key)
    log.debug(
        "Data range for uuid %s, key %s: %s to %s", uuid, key, start_date, end_date
    )
    months_to_prepare = get_monthly_utc_date_range_array_from_(
        start_date=start_date, end_date=end_date
    )

    for month in months_to_prepare:
        log.debug(f"Preparing co-indexing data for month {month}")
        start_date = month["start_date"]
        end_date = month["end_date"]
        result = fetch_data(
            api_instance=api,
            uuid=uuid,
            key=key,
            start_date=start_date,
            end_date=end_date,
            start_depth=-1.0,
            end_depth=-1.0,
            columns=["TIME", "LATITUDE", "LONGITUDE"],
        )

        response = async_response_json(result=result, compress=False)
        json_result = response.body
        bucket = config.get_csv_bucket_name()
        s3_key = (
            f"co-index-prep/{uuid}/{key}/{start_date.year}/{start_date.month:02d}.json"
        )

        # json_result is already bytes, so create BytesIO directly
        json_file_obj = BytesIO(json_result)

        # Upload to S3
        download_url = aws.upload_fileobj_to_s3(
            file_obj=json_file_obj, s3_bucket=bucket, s3_key=s3_key
        )

        log.info(f"Uploaded JSON data to S3: {download_url}")


if __name__ == "__main__":
    param = [
        {
            "uuid": "78d588ed-79dd-47e2-b806-d39025194e7e",
            "key": "mooring_satellite_altimetry_calibration_validation.parquet",
        }
    ]
    prepare_co_indexing_data(0, {Parameters.INDEX_DATASETS.value: json.dumps(param)})
