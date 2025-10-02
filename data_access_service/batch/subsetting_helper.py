from typing import List

import pandas as pd

from data_access_service.batch.batch_enums import Parameters
from data_access_service.models.multi_polygon_helper import MultiPolygonHelper
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.server import api_setup, app
from data_access_service.utils.date_time_utils import ensure_timezone


def get_keys(parameters) -> list[str]:
    if (
        Parameters.KEY.value in parameters
        and parameters[Parameters.KEY.value] is not None
    ):
        return [item.strip() for item in parameters[Parameters.KEY.value].split(",")]
    else:
        return ["*"]


def get_uuid(parameters) -> str:
    return parameters[Parameters.UUID.value]


def get_subset_request(parameters) -> SubsetRequest:
    uuid = get_uuid(parameters)
    keys = get_keys(parameters)
    start_date = parameters[Parameters.START_DATE.value]
    end_date = parameters[Parameters.END_DATE.value]
    recipient = parameters[Parameters.RECIPIENT.value]
    bboxes = MultiPolygonHelper(
        multi_polygon=(parameters[Parameters.MULTI_POLYGON.value])
    ).bboxes

    return SubsetRequest(
        uuid=uuid,
        keys=keys,
        start_date=start_date,
        end_date=end_date,
        bbox=bboxes,
        recipient=recipient,
    )


def trim_date_range_for_keys(
    uuid: str,
    keys: List[str],
    requested_start_date: pd.Timestamp,
    requested_end_date: pd.Timestamp,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    api = api_setup(app)

    # convert into utc:
    requested_start_date = ensure_timezone(requested_start_date)
    requested_end_date = ensure_timezone(requested_end_date)

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

    # if the requested dates are outside the available range, trim them
    trimmed_start_date = requested_start_date
    trimmed_end_date = requested_end_date
    if requested_start_date < min_date_of_keys:
        trimmed_start_date = min_date_of_keys
    if requested_end_date > max_date_of_keys:
        trimmed_end_date = max_date_of_keys

    return trimmed_start_date, trimmed_end_date
