import json
import os
from datetime import datetime
from typing import List, Dict

from data_access_service import API, init_log, Config
from data_access_service.core.AWSClient import AWSClient
from data_access_service.models.data_file_factory import DataFileFactory
from data_access_service.tasks.data_file_upload import (
    upload_all_files_in_folder_to_temp_s3,
)
from data_access_service.utils.date_time_utils import (
    get_monthly_date_range_array_from_,
    trim_date_range,
)

efs_mount_point = "/mount/efs/"

config: Config = Config.get_config()
log = init_log(config)


def process_data_files(
    job_id_of_init: str,
    uuid: str,
    key: str,
    start_date: datetime,
    end_date: datetime,
    multi_polygon: str,
) -> str | None:
    tmp_data_folder_path = config.get_temp_folder(job_id_of_init)
    multi_polygon_dict = json.loads(multi_polygon)

    if None in [uuid, key, start_date, end_date, json.loads(multi_polygon)]:
        raise ValueError("One or more required arguments are None")

    aws = AWSClient()
    log.info(f"Start prepare {uuid}-{key}")

    try:
        generate_csv_files(
            tmp_data_folder_path, uuid, key, start_date, end_date, multi_polygon_dict
        )
        upload_all_files_in_folder_to_temp_s3(
            master_job_id=job_id_of_init, local_folder=tmp_data_folder_path, aws=aws
        )

    except TypeError as e:
        log.error(f"Error: {e}")
    except ValueError as e:
        log.error(f"Error: {e}")
    except Exception as e:
        log.error(f"Error: {e}")
    return None


def generate_csv_files(
    folder_path: str,
    uuid: str,
    key: str,
    start_date: datetime,
    end_date: datetime,
    multi_polygon: dict,
):
    api = API()
    # Use sync init, it does not matter the load is slow as we run in batch
    api.initialize_metadata()

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
            key=key,
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
                    key,
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
    key: str,
    start_date: datetime,
    end_date: datetime,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
):
    log.info(
        f"Querying data for uuid={uuid}, key={key}, start_date={start_date}, end_date={end_date}, "
    )
    log.info(
        f"lat_min={min_lat}, lat_max={max_lat}, lon_min={min_lon}, lon_max={max_lon}"
    )

    df = None
    try:
        df = api.get_dataset_data(
            uuid=uuid,
            key=key,
            date_start=start_date,
            date_end=end_date,
            lat_min=min_lat,
            lat_max=max_lat,
            lon_min=min_lon,
            lon_max=max_lon,
        )
    except ValueError as e:
        log.info(f"seems like no data for this polygon. Error: {e}")
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
