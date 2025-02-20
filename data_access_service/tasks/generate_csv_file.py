import datetime
import json
import logging
from typing import List, Dict

import pandas as pd

from data_access_service import API, init_log
from data_access_service.core.AWSClient import AWSClient
from data_access_service.utils.email_generator import (
    generate_started_email_subject,
    generate_started_email_content,
    generate_completed_email_subject,
    generate_completed_email_content,
)

log = logging.getLogger(__name__)


def process_csv_data_file(
    uuid: str,
    start_date: str,
    end_date: str,
    multi_polygon: str,
    recipient: str,
):
    init_log(logging.DEBUG)

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

    # disable the starting email for now and move it to the ogcapi to make it faster
    # startingSubject = generate_started_email_subject(uuid)
    # startingContent = generate_started_email_content(uuid, conditions)

    # aws.send_email(recipient, startingSubject, startingContent)

    try:
        # generate csv file and upload to s3
        csv_file_path = _generate_csv_file(
            start_date, end_date, multi_polygon_dict, uuid
        )
        s3_path = f"{uuid}/{csv_file_path}"
        object_url = aws.upload_data_file_to_s3(csv_file_path, s3_path)

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


def _generate_csv_file(
    start_date: datetime, end_date: datetime, multi_polygon: dict, uuid: str
):

    api = API()
    data_frame = None

    # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
    #  we can change to use the polygon coordinates directly
    for polygon in multi_polygon["coordinates"]:
        lats_lons = _get_lat_lon_from_(polygon)
        min_lat = lats_lons["min_lat"]
        max_lat = lats_lons["max_lat"]
        min_lon = lats_lons["min_lon"]
        max_lon = lats_lons["max_lon"]

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
            continue
        except Exception as e:
            log.error(f"Error: {e}")

        if df is not None and not df.empty:
            data_frame = pd.concat([data_frame, df], ignore_index=True)

    if data_frame is None or data_frame.empty:
        raise ValueError(
            f" No data found for uuid={uuid}, start_date={start_date}, end_date={end_date}, multi_polygon={multi_polygon}"
        )

    csv_file_path = f"date:{start_date}~{end_date}.csv"
    data_frame.to_csv(csv_file_path, index=False)

    return csv_file_path


def _query_data(end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid):

    api = API()

    data_frame = api.get_dataset_data(
        uuid=uuid,
        date_start=start_date,
        date_end=end_date,
        lat_min=min_lat,
        lat_max=max_lat,
        lon_min=min_lon,
        lon_max=max_lon,
    )

    if data_frame is None or data_frame.empty:
        raise ValueError(
            f"One or more required arguments are None: uuid={uuid}, start_date={start_date}, end_date={end_date}, min_lat={min_lat}, max_lat={max_lat}, min_lon={min_lon}, max_lon={max_lon}"
        )
    return data_frame


def _get_lat_lon_from_(polygon: List[List[List[float]]]) -> Dict[str, float]:
    coordinates = [coord for ring in polygon for coord in ring]
    lats = [coord[1] for coord in coordinates]
    lons = [coord[0] for coord in coordinates]

    return {
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
    }
