import datetime
import json
import logging
import os
import shutil
from typing import List, Dict

from data_access_service import API, init_log
from data_access_service.core.AWSClient import AWSClient
from data_access_service.models.data_file_factory import DataFileFactory
from data_access_service.utils.date_time_utils import (
    get_monthly_date_range_array_from_,
    parse_date,
    YEAR_MONTH_DAY,
)
from data_access_service.utils.email_generator import (
    generate_completed_email_subject,
    generate_completed_email_content,
)
from data_access_service.utils.file_utils import zip_the_folder

log = logging.getLogger(__name__)


#  below is for local testing
# efs_mount_point = ""

efs_mount_point = "/mount/efs/"


def process_csv_data_file(
    job_id: str,
    uuid: str,
    start_date: str,
    end_date: str,
    multi_polygon: str,
    recipient: str,
):
    init_log(logging.DEBUG)

    # TODO: put these folders for now and will be replaced when start doing RO-Crate   format
    job_root_folder = efs_mount_point + job_id + "/"
    data_folder_path = job_root_folder + "data/"
    # data_folder_path = efs_mount_point

    multi_polygon_dict = json.loads(multi_polygon)

    if None in [uuid, start_date, end_date, json.loads(multi_polygon), recipient]:
        raise ValueError("One or more required arguments are None")

    aws = AWSClient()
    log.info("start " + uuid)

    conditions = [
        ("start date", start_date),
        ("end date", end_date),
        ("polygon", multi_polygon),
    ]

    try:
        start_date = parse_date(start_date, YEAR_MONTH_DAY)
        end_date = parse_date(end_date, YEAR_MONTH_DAY)

        # generate csv file and upload to s3
        generate_csv_files(
            data_folder_path, start_date, end_date, multi_polygon_dict, uuid
        )

        data_file_zip_path = generate_zip_name(uuid, start_date, end_date)
        zipped_file_path = zip_the_folder(
            folder_path=job_root_folder,
            output_zip_path=f"{job_root_folder}/{data_file_zip_path}",
        )
        log.info(f"Zipped file path: {zipped_file_path}")

        log.info(f"Uploading zip file to S3: {data_file_zip_path}.zip")
        s3_path = f"{uuid}/{zipped_file_path}"
        object_url = aws.upload_data_file_to_s3(zipped_file_path, s3_path)

        # clean up the folder
        log.info(f"Cleaning up folder: {job_root_folder}")
        try:
            shutil.rmtree(job_root_folder)
            log.info(f"Successfully cleaned up folder: {job_root_folder}")
        except Exception as e:
            log.error(f"Error cleaning up folder: {e}")

        # send email to recipient
        finishingSubject = generate_completed_email_subject(uuid)
        finishingContent = generate_completed_email_content(
            uuid, conditions, object_url
        )
        aws.send_email(recipient, finishingSubject, finishingContent)

    except TypeError as e:
        log.error(f"Error: {e}")
        aws.send_email(recipient, "Error", "Type Error occurred.")
    except ValueError as e:
        log.error(f"Error: {e}")
        aws.send_email(recipient, "Error", "No data found for selected conditions")
    except Exception as e:
        log.error(f"Error: {e}")
        aws.send_email(recipient, "Error", "An error occurred.")


def trim_date_range(
    api: API, uuid: str, requested_start_date: datetime, requested_end_date: datetime
) -> (datetime, datetime):

    log.info(f"Original date range: {requested_start_date} to {requested_end_date}")
    metadata_temporal_extent = api.get_temporal_extent(uuid=uuid)
    if len(metadata_temporal_extent) != 2:
        raise ValueError(
            f"Invalid metadata temporal extent: {metadata_temporal_extent}"
        )

    metadata_start_date, metadata_end_date = metadata_temporal_extent

    metadata_start_date = metadata_start_date.replace(tzinfo=None)
    metadata_end_date = metadata_end_date.replace(tzinfo=None)
    if requested_start_date < metadata_start_date:
        requested_start_date = metadata_start_date
    if requested_end_date > metadata_end_date:
        requested_end_date = metadata_end_date

    log.info(f"Trimmed date range: {requested_start_date} to {requested_end_date}")
    return requested_start_date, requested_end_date


def generate_csv_files(
    folder_path: str,
    start_date: datetime,
    end_date: datetime,
    multi_polygon: dict,
    uuid: str,
):

    api = API()

    # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
    #  we can change to use the polygon coordinates directly
    for polygon in multi_polygon["coordinates"]:
        lats_lons = get_lat_lon_from_(polygon)
        min_lat = lats_lons["min_lat"]
        max_lat = lats_lons["max_lat"]
        min_lon = lats_lons["min_lon"]
        max_lon = lats_lons["max_lon"]

        dataFactory = DataFileFactory(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            log=log,
        )

        start_date, end_date = trim_date_range(
            api=api,
            uuid=uuid,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )

        # date_ranges = get_yearly_date_range_array_from_(start_date=start_date, end_date=end_date)
        date_ranges = get_monthly_date_range_array_from_(
            start_date=start_date, end_date=end_date
        )
        for date_range in date_ranges:
            df = query_data(
                api,
                uuid,
                date_range.start_date,
                date_range.end_date,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            )
            if df is not None and not df.empty:
                dataFactory.add_data(df, date_range.start_date, date_range.end_date)
                if dataFactory.is_full():
                    try:
                        dataFactory.save_as_csv_in_folder_(folder_path)
                    except Exception as e:
                        log.error(f"Error saving data: {e}", exc_info=True)
                        log.error(e)

        # save the last data frame
        if dataFactory.data_frame is not None:
            dataFactory.save_as_csv_in_folder_(folder_path)

    if not any(os.scandir(folder_path)):
        raise ValueError(
            f" No data found for uuid={uuid}, start_date={start_date}, end_date={end_date}, multi_polygon={multi_polygon}"
        )


def query_data(
    api,
    uuid: str,
    start_date: datetime,
    end_date: datetime,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
):
    df = None
    log.info(
        f"Querying data for uuid={uuid}, start_date={start_date}, end_date={end_date}, "
    )
    log.info(
        f"lat_min={min_lat}, lat_max={max_lat}, lon_min={min_lon}, lon_max={max_lon}"
    )
    try:
        df = api.get_dataset_data(
            uuid=uuid,
            date_start=start_date,
            date_end=end_date,
            lat_min=min_lat,
            lat_max=max_lat,
            lon_min=min_lon,
            lon_max=max_lon,
        )
    except ValueError as e:
        log.info("seems like no data for this polygon", e)
    except Exception as e:
        log.error(f"Error: {e}")

    if df is not None and not df.empty:
        return df
    else:
        log.info("No data found for the given parameters")
        return None


def get_lat_lon_from_(polygon: List[List[List[float]]]) -> Dict[str, float]:
    coordinates = [coord for ring in polygon for coord in ring]
    lats = [coord[1] for coord in coordinates]
    lons = [coord[0] for coord in coordinates]

    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
    }


def generate_zip_name(uuid, start_date, end_date):
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    return f"{uuid}_{start_date_str}_{end_date_str}"
