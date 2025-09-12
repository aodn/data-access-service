from typing import List

import pandas as pd

from data_access_service.batch.batch_enums import Parameters
from data_access_service.server import api_setup, app


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


def trim_date_range_for_keys(
    uuid: str,
    keys: List[str],
    requested_start_date: pd.Timestamp,
    requested_end_date: pd.Timestamp,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    api = api_setup(app)

    # convert into utc:
    if requested_start_date.tzinfo is not None:
        requested_start_date = requested_start_date.tz_convert("UTC")
    else:
        requested_start_date = requested_start_date.tz_localize("UTC")
    if requested_end_date.tzinfo is not None:
        requested_end_date = requested_end_date.tz_convert("UTC")
    else:
        requested_end_date = requested_end_date.tz_localize("UTC")

    min_date_of_keys = pd.Timestamp.now(tz="UTC")
    max_date_of_keys = pd.Timestamp("1970-01-01 00:00:00.000000000", tz="UTC")

    # get the union spatial extents of all selected keys
    for key in keys:
        start_date, end_date = api.get_temporal_extent(uuid, key)
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
