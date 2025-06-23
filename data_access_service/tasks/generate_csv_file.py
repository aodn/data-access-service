import json
import os
import dask.dataframe as dd

from datetime import datetime
from typing import List, Dict, Optional
from data_access_service import API, init_log, Config
from data_access_service.core.AWSClient import AWSClient
from data_access_service.core.descriptor import Descriptor
from data_access_service.utils.date_time_utils import YEAR_MONTH_DAY
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

api = API()


def process_data_files(
    job_id_of_init: str,
    uuid: str,
    keys: List[str],
    start_date: datetime,
    end_date: datetime,
    multi_polygon: str | None,
) -> str | None:
    if multi_polygon is not None:
        multi_polygon_dict = json.loads(multi_polygon)
    else:
        multi_polygon_dict = None

    # Use sync init, it does not matter the load is slow as we run in batch
    if not api.get_api_status():
        api.initialize_metadata()

    if None in [uuid, keys, start_date, end_date]:
        raise ValueError("One or more required arguments are None")

    if "*" in keys:
        # We need to expand to include all filename as key give "*" as wildcard
        md: Dict[str, Descriptor] = api.get_mapped_meta_data(uuid)
        # key are all file name associated given UUID
        dataset = md.keys()
    else:
        dataset = keys

    aws = AWSClient()

    try:
        for datum in dataset:
            log.info(f"Start prepare {uuid}-{datum}")
            tmp_data_folder_path = config.get_temp_folder(job_id_of_init, datum)
            _generate_csv_files_polygon(
                tmp_data_folder_path,
                uuid,
                datum,
                start_date,
                end_date,
                multi_polygon_dict,
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


def _generate_file_name(
    folder_path: str,
    start_date: datetime,
    end_date: datetime,
    min_lat: int = -90,
    max_lat: int = 90,
    min_lon: int = -180,
    max_lon: int = 180,
):
    min_lat = -90 if min_lat is None else min_lat
    max_lat = 90 if max_lat is None else max_lat

    min_lon = -180 if min_lon is None else min_lon
    max_lon = 180 if max_lon is None else max_lon

    return f"{folder_path}/date_{start_date.strftime(YEAR_MONTH_DAY)}_{end_date.strftime(YEAR_MONTH_DAY)}_bbox_{int(round(min_lon, 0))}_{int(round(min_lat, 0))}_{int(round(max_lon, 0))}_{int(round(max_lat, 0))}.csv"


def _generate_csv_files(
    folder_path: str,
    uuid: str,
    key: str,
    start_date: datetime,
    end_date: datetime,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
):
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

        combined_result: dd.DataFrame | None = None

        for date_range in date_ranges:
            result: Optional[dd.DataFrame] = query_data(
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
            if result is not None:
                if combined_result is None:
                    combined_result = result
                else:
                    combined_result = dd.concat([combined_result, result])

        if combined_result is not None:
            csv_file_path = _generate_file_name(
                folder_path, start_date, end_date, min_lat, max_lat, min_lon, max_lon
            )
            dd.to_csv(combined_result, filename=csv_file_path, index=False)


def _generate_csv_files_polygon(
    folder_path: str,
    uuid: str,
    key: str,
    start_date: datetime,
    end_date: datetime,
    multi_polygon: dict | None,
):
    # Use sync init, it does not matter the load is slow as we run in batch
    if not api.get_api_status():
        api.initialize_metadata()

    if multi_polygon is not None:
        # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
        #  we can change to use the polygon coordinates directly
        for polygon in multi_polygon["coordinates"]:
            lats_lons = get_lat_lon_from_(polygon)
            min_lat = lats_lons["min_lat"]
            max_lat = lats_lons["max_lat"]
            min_lon = lats_lons["min_lon"]
            max_lon = lats_lons["max_lon"]

            _generate_csv_files(
                folder_path,
                uuid,
                key,
                start_date,
                end_date,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            )
    else:
        _generate_csv_files(
            folder_path, uuid, key, start_date, end_date, None, None, None, None
        )

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

    if df is not None:
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
