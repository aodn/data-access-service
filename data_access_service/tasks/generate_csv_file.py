import datetime
import logging

import pandas as pd
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from dateutil import parser

from data_access_service import API, init_log
from data_access_service.core.AWSClient import AWSClient
from data_access_service.tasks.email_generator import (
    generate_started_email_subject,
    generate_started_email_content,
    generate_completed_email_subject,
    generate_completed_email_content,
)

log = logging.getLogger(__name__)


def process_csv_data_file2(
    uuid: str,
    start_date: str,
    end_date: str,
    multi_polygon: MultiPolygon,
    recipient: str,
):
    init_log(logging.DEBUG)
    # log all params for testing
    log.info(f"uuid: {uuid}")
    log.info(f"start_date: {start_date}")
    log.info(f"end_date: {end_date}")
    log.info(f"multi_polygon: {multi_polygon}")
    log.info(f"recipient: {recipient}")

    # if None in [uuid, start_date, end_date, multi_polygon, recipient]:
    #     raise ValueError("One or more required arguments are None")
    #
    # aws = AWSClient()
    # log.info("start " + uuid)
    #
    # conditions = [
    #     ("start date", start_date),
    #     ("end date", end_date),
    #     ("polygon", multi_polygon.wkt)
    # ]
    #
    # startingSubject = generate_started_email_subject(uuid)
    # startingContent = generate_started_email_content(uuid, conditions)
    #
    # aws.send_email(recipient, startingSubject, startingContent)
    # # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
    # #  we can change to use the polygon coordinates directly
    # for polygon in multi_polygon.geoms:
    #     log.info(f"Processing polygon with {len(polygon.exterior.coords)} exterior coordinates")
    #     if not is_rectangle(polygon):
    #         raise ValueError("Only rectangles are supported for current version")
    #     min_lon, min_lat, max_lon, max_lat = polygon.bounds


def process_csv_data_file(
    uuid, start_date, end_date, min_lat, max_lat, min_lon, max_lon, recipient
):
    init_log(logging.DEBUG)

    # for debug usage. just keep it for several days
    if uuid is None:
        uuid = "debug-uuid"
    if start_date is None:
        start_date = "debug-start-date"
    if end_date is None:
        end_date = "debug-end-date"
    if min_lat is None:
        min_lat = "-90"
    if max_lat is None:
        max_lat = "90"
    if min_lon is None:
        min_lon = "-180"
    if max_lon is None:
        max_lon = "180"

    if None in [uuid, start_date, end_date]:
        raise ValueError("One or more required arguments are None")

    aws = AWSClient()
    log.info("start " + uuid)

    # generate a condition list including all existing conditions
    conditions = [
        ("start date", start_date),
        ("end date", end_date),
        ("min latitude", min_lat),
        ("max latitude", max_lat),
        ("min longitude", min_lon),
        ("max longitude", max_lon),
    ]

    startingSubject = generate_started_email_subject(uuid)
    startingContent = generate_started_email_content(uuid, conditions)

    aws.send_email(recipient, startingSubject, startingContent)

    start_date = parse_date(start_date)
    end_date = parse_date(end_date)

    csv_file_path = _generate_csv_file(
        end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid
    )

    s3_path = f"{uuid}/{csv_file_path}"

    object_url = aws.upload_data_file_to_s3(csv_file_path, s3_path)
    finishingSubject = generate_completed_email_subject(uuid)
    finishingContent = generate_completed_email_content(uuid, conditions, object_url)
    aws.send_email(recipient, finishingSubject, finishingContent)


def _generate_csv_file(end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid):

    data_frame = _query_data(
        end_date, max_lat, max_lon, min_lat, min_lon, start_date, uuid
    )

    csv_file_path = f"lat:{min_lat}~{max_lat}_lon:{min_lon}~{max_lon}_date:{start_date}~{end_date}.csv"
    data_frame.to_csv(csv_file_path, index=False)

    return csv_file_path


def _generate_csv_file2(
    start_date: datetime, end_date: datetime, multi_polygon: MultiPolygon, uuid: str
):

    api = API()
    data_frame = None

    # TODO: currently, assume polygons are all rectangles. when cloud-optimized library is upgraded,
    #  we can change to use the polygon coordinates directly
    for polygon in multi_polygon.geoms:
        log.info(
            f"Processing polygon with {len(polygon.exterior.coords)} exterior coordinates"
        )
        if not is_rectangle(polygon):
            raise ValueError("Only rectangles are supported for current version")
        min_lon, min_lat, max_lon, max_lat = polygon.bounds
        df = api.get_dataset_data(
            uuid=uuid,
            date_start=start_date,
            date_end=end_date,
            lat_min=min_lat,
            lat_max=max_lat,
            lon_min=min_lon,
            lon_max=max_lon,
        )
        data_frame = pd.concat([data_frame, df], ignore_index=True)


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


# TODO: please remove this function after the cloud-optimized library is upgraded to support non-rectangular polygons
def is_rectangle(polygon: Polygon) -> bool:
    if len(polygon.exterior.coords) != 5:
        return False  # A rectangle has 5 coordinates (4 corners + 1 repeated start/end point)

    coords = list(polygon.exterior.coords)
    for i in range(4):
        p1, p2, p3 = coords[i], coords[(i + 1) % 4], coords[(i + 2) % 4]
        if not is_right_angle(p1, p2, p3):
            return False

    return True


# TODO: please remove this function after the cloud-optimized library is upgraded to support non-rectangular polygons
def is_right_angle(p1, p2, p3) -> bool:
    # Check if the angle between p1-p2 and p2-p3 is 90 degrees
    dx1, dy1 = p1[0] - p2[0], p1[1] - p2[1]
    dx2, dy2 = p3[0] - p2[0], p3[1] - p2[1]
    dot_product = dx1 * dx2 + dy1 * dy2
    return dot_product == 0


def parse_date(date_string: str) -> datetime:
    parsed_date = parser.parse(date_string)
    return parsed_date.strftime("%Y-%m-%d")
