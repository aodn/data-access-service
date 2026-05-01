from typing import List

import pandas as pd

from data_access_service import API
from data_access_service.batch.batch_enums import Parameters
from data_access_service.models.multi_polygon_helper import MultiPolygonHelper
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.server import api_setup, app
from data_access_service.utils.date_time_utils import ensure_timezone


def get_keys(parameters: dict) -> list[str]:
    """Parse the comma-separated `key` parameter; ['*'] when absent."""
    raw = parameters.get(Parameters.KEY.value)
    if raw is None:
        return ["*"]
    return [item.strip() for item in raw.split(",")]


def get_subset_request(parameters: dict) -> SubsetRequest:
    multi_polygon = parameters[Parameters.MULTI_POLYGON.value]
    return SubsetRequest(
        uuid=parameters[Parameters.UUID.value],
        keys=get_keys(parameters),
        start_date=parameters[Parameters.START_DATE.value],
        end_date=parameters[Parameters.END_DATE.value],
        recipient=parameters[Parameters.RECIPIENT.value],
        multi_polygon=multi_polygon,
        bboxes=MultiPolygonHelper(multi_polygon=multi_polygon).bboxes,
        collection_title=parameters.get(Parameters.COLLECTION_TITLE.value),
        full_metadata_link=parameters.get(Parameters.FULL_METADATA_LINK.value),
        suggested_citation=parameters.get(Parameters.SUGGESTED_CITATION.value),
        output_format=parameters.get(Parameters.OUTPUT_FORMAT.value),
    )


def trim_date_range_for_keys(
    api: API,
    uuid: str,
    keys: List[str],
    requested_start_date: pd.Timestamp,
    requested_end_date: pd.Timestamp,
) -> tuple[pd.Timestamp, pd.Timestamp] | tuple[None, None]:

    # convert into utc:
    requested_start_date = ensure_timezone(requested_start_date)
    requested_end_date = ensure_timezone(requested_end_date)

    # throw error if requested start date is after requested end date
    if requested_start_date > requested_end_date:
        raise ValueError(
            f"Requested start date {requested_start_date} is after requested end date {requested_end_date}"
        )

    min_date_of_keys = pd.Timestamp.now(tz="UTC")
    max_date_of_keys = pd.Timestamp("1970-01-01 00:00:00.000000000", tz="UTC")

    # get the union spatial extents of all selected keys
    for key in keys:
        start_date, end_date = api.get_temporal_extent(uuid, key)
        if start_date is None or end_date is None:
            # if didn't get the temporal extent (e.g. when testing) just return the requested dates
            return requested_start_date, requested_end_date
        start_date = ensure_timezone(start_date)
        end_date = ensure_timezone(end_date)
        if start_date < min_date_of_keys:
            min_date_of_keys = start_date
        if end_date > max_date_of_keys:
            max_date_of_keys = end_date

    # if the requested date range is completely outside the available range, return None
    if requested_end_date < min_date_of_keys or requested_start_date > max_date_of_keys:
        return None, None

    # if the requested date ranges are bigger the available range, trim them
    trimmed_start_date = requested_start_date
    trimmed_end_date = requested_end_date
    if requested_start_date < min_date_of_keys:
        trimmed_start_date = min_date_of_keys
    if requested_end_date > max_date_of_keys:
        trimmed_end_date = max_date_of_keys

    # make sure the trimmed dates includes nanoseconds. end date should be  the final nanosecond
    trimmed_start_date = trimmed_start_date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        nanosecond=0,
    )
    trimmed_end_date = trimmed_end_date.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
        nanosecond=999,
    )
    return trimmed_start_date, trimmed_end_date
