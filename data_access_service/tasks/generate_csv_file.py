import json
import os
import shutil
from datetime import datetime, time
from typing import List, Dict

from numpy.f2py.auxfuncs import throw_error

from data_access_service import API, init_log, Config
from data_access_service.core.AWSClient import AWSClient
from data_access_service.models.data_file_factory import DataFileFactory
from data_access_service.tasks.upload_data_files import upload_all_files_in_folder_to_temp_s3
from data_access_service.utils.date_time_utils import (
    get_monthly_date_range_array_from_,
    trim_date_range,
    parse_date,
)
from data_access_service.utils.email_generator import (
    generate_completed_email_subject,
    generate_completed_email_content,
)

efs_mount_point = "/mount/efs/"


def process_data_files(
    job_id: str,
    uuid: str,
    start_date: datetime,
    end_date: datetime,
    multi_polygon: str,
    recipient: str,
) -> str | None:
    config: Config = Config.get_config()
    log = init_log(config)

    tmp_data_folder_path = config.get_temp_folder(job_id)
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
        # start_date = parse_date(start_date)
        # end_date = parse_date(end_date, time(23, 59, 59))

        # generate csv file and upload to s3
        generate_csv_files(
            tmp_data_folder_path, start_date, end_date, multi_polygon_dict, uuid
        )
        upload_all_files_in_folder_to_temp_s3(job_id=job_id, local_folder=tmp_data_folder_path, aws=aws)

        # data_file_zip_path = generate_zip_name(uuid, start_date, end_date)
        # object_url = aws.zip_directory_to_s3(
        #     tmp_data_folder_path,
        #     config.get_csv_bucket_name(),
        #     job_id + ".zip",
        # )
        #
        # # send email to recipient
        # finishing_subject = generate_completed_email_subject(uuid)
        # finishing_content = generate_completed_email_content(
        #     uuid, conditions, object_url
        # )
        # aws.send_email(recipient, finishing_subject, finishing_content)

    except TypeError as e:
        log.error(f"Error: {e}")
        aws.send_email(recipient, "Error", "Type Error occurred.")
    except ValueError as e:
        log.error(f"Error: {e}")
        aws.send_email(recipient, "Error", "No data found for selected conditions")
    except Exception as e:
        log.error(f"Error: {e}")
        aws.send_email(recipient, "Error", "An error occurred.")
    # finally:
        # shutil.rmtree(tmp_data_folder_path)
    return None

def generate_csv_files(
    folder_path: str,
    start_date: datetime,
    end_date: datetime,
    multi_polygon: dict,
    uuid: str,
):
    log = init_log(Config.get_config())
    api = API()

    # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
    #  we can change to use the polygon coordinates directly
    for polygon in multi_polygon["coordinates"]:
        lats_lons = get_lat_lon_from_(polygon)
        min_lat = lats_lons["min_lat"]
        max_lat = lats_lons["max_lat"]
        min_lon = lats_lons["min_lon"]
        max_lon = lats_lons["max_lon"]

        data_factory = DataFileFactory(
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

        if start_date is not None and end_date is not None:
            date_ranges = get_monthly_date_range_array_from_(
                start_date=start_date, end_date=end_date
            )
            for date_range in date_ranges:
                df = query_data(
                    api,
                    uuid,
                    date_range["start_date"],
                    date_range["end_date"],
                    min_lat,
                    max_lat,
                    min_lon,
                    max_lon,
                )
                if df is not None and not df.empty:
                    data_factory.add_data(
                        df, date_range["start_date"], date_range["end_date"]
                    )
                    if data_factory.is_full():
                        try:
                            data_factory.save_as_csv_in_folder_(folder_path)
                        except Exception as e:
                            log.error(f"Error saving data: {e}", exc_info=True)
                            log.error(e)

            # save the last data frame
            if data_factory.data_frame is not None:
                data_factory.save_as_csv_in_folder_(folder_path)

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
    log = init_log(Config.get_config())

    log.info(
        f"Querying data for uuid={uuid}, start_date={start_date}, end_date={end_date}, "
    )
    log.info(
        f"lat_min={min_lat}, lat_max={max_lat}, lon_min={min_lon}, lon_max={max_lon}"
    )

    df = None
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
