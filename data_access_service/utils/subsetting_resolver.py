"""Shared "apply the user's subset" behaviour.

Every consumer of a subset request must interpret it the same way: which
keys, which date range, which area. This module is the single owner of that
interpretation, so the consumers cannot drift apart.
"""

import json
from dataclasses import dataclass, replace
from typing import List, Optional, Tuple, Union

import pandas as pd

from data_access_service.core.constants import UNIX_EPOCH_UTC, WHOLE_GLOBE_BBOX
from data_access_service.models.bounding_box import BoundingBox
from data_access_service.models.subset_request import NON_SPECIFIED, SubsetRequest
from data_access_service.utils.multi_polygon_helper import MultiPolygonHelper


@dataclass(frozen=True)
class ResolvedSubset:
    """A user's subset request after every "resolve" step has been applied.

    This is the single input every consumer works from, so they can never
    disagree about WHAT is being subset.
    """

    uuid: str
    keys: List[str]
    start_date: Optional[pd.Timestamp]
    end_date: Optional[pd.Timestamp]
    bboxes: List[BoundingBox]
    columns: Optional[List[str]] = None

    @property
    def has_data(self) -> bool:
        """False when the requested date range is entirely outside the
        dataset's temporal extent (the download would produce no data)."""
        return self.start_date is not None and self.end_date is not None

    @property
    def effective_bboxes(self) -> List[BoundingBox]:
        """The bboxes to slice with: empty means "no spatial filter", which
        becomes one whole-globe bbox. Explicit bounds (not open/None slices)
        because the batch mask path compares values against them
        (subset_zarr.form_mask). Both the batch download and the size
        estimate must slice from THIS list so they select the same region.
        """
        return self.bboxes or [WHOLE_GLOBE_BBOX]


def resolve_subset(
    api,
    uuid: str,
    keys: Optional[List[str]],
    start_date_str: str,
    end_date_str: str,
    multi_polygon: Union[str, dict, None],
    columns: Optional[List[str]] = None,
) -> ResolvedSubset:
    """Interpret a raw subset request into a ResolvedSubset.

    Steps:
    1. resolve keys: expand "*" / absent keys to all keys of the dataset
    2. resolve dates: defaults, day + nano precision, trim to the union
       temporal extent of the resolved keys
    3. resolve bboxes: parse the GeoJSON MultiPolygon into bounding boxes

    :raises ValueError/TypeError: on unparseable dates or a bad multi_polygon
    """
    resolved_keys = resolve_keys(api, uuid, keys)
    start_date, end_date = resolve_date_range(
        start_date_str, end_date_str, api=api, uuid=uuid, keys=resolved_keys
    )
    bboxes = resolve_bboxes(multi_polygon)

    return ResolvedSubset(
        uuid=uuid,
        keys=resolved_keys,
        start_date=start_date,
        end_date=end_date,
        bboxes=bboxes,
        columns=columns,
    )


def normalize_request(api, subset_request: SubsetRequest) -> SubsetRequest:
    """Return the request with "*" keys expanded and non-specified dates
    resolved to defaults, so every later reader (child jobs, result emails)
    sees the actual values instead of the raw user input.
    """
    from data_access_service.utils.date_time_utils import resolve_non_specified_dates

    start_date, end_date = resolve_non_specified_dates(
        subset_request.start_date, subset_request.end_date
    )
    keys = resolve_keys(api, subset_request.uuid, subset_request.keys)
    if (
        start_date == subset_request.start_date
        and end_date == subset_request.end_date
        and keys == subset_request.keys
    ):
        return subset_request
    return replace(subset_request, start_date=start_date, end_date=end_date, keys=keys)


def resolve_keys(api, uuid: str, keys: Optional[List[str]]) -> List[str]:
    """Expand "*" / absent keys to all keys of the dataset."""
    if not keys or "*" in keys:
        return list((api.get_mapped_meta_data(uuid) or {}).keys())
    return list(keys)


def resolve_date_range(
    start_date_str: str,
    end_date_str: str,
    api=None,
    uuid: Optional[str] = None,
    keys: Optional[List[str]] = None,
) -> Tuple[pd.Timestamp, pd.Timestamp] | Tuple[None, None]:
    """Resolve the requested dates into the range the subset will actually use.

    Steps:
    1. resolve "non-specified" dates to defaults
    2. parse with day + nano precision supplied (end date becomes the final
       nanosecond)
    3. when api and keys are given, trim to the union temporal extent of the
       keys - (None, None) means the request is entirely outside it

    Without api/keys it is a pure string-to-Timestamp conversion (steps 1-2).
    """
    from data_access_service.utils.date_time_utils import (
        resolve_non_specified_dates,
        supply_day_with_nano_precision,
    )

    start_str, end_str = resolve_non_specified_dates(start_date_str, end_date_str)
    start_date, end_date = supply_day_with_nano_precision(start_str, end_str)

    if api is not None and keys:
        start_date, end_date = trim_date_range_for_keys(
            api=api,
            uuid=uuid,
            keys=keys,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
    return start_date, end_date


def resolve_bboxes(multi_polygon: Union[str, dict, None]) -> List[BoundingBox]:
    """Parse a GeoJSON MultiPolygon (string or already-parsed dict) into
    bounding boxes. Returns [] when there is no spatial filter."""
    if multi_polygon is None or multi_polygon == NON_SPECIFIED:
        return []
    if isinstance(multi_polygon, dict):
        multi_polygon = json.dumps(multi_polygon)
    return MultiPolygonHelper(multi_polygon=multi_polygon).bboxes


def trim_date_range_for_keys(
    api,
    uuid: str,
    keys: List[str],
    requested_start_date: pd.Timestamp,
    requested_end_date: pd.Timestamp,
) -> Tuple[pd.Timestamp, pd.Timestamp] | Tuple[None, None]:
    """Trim the requested date range to the union temporal extent of the given
    keys. Returns (None, None) when the request is entirely outside it."""
    from data_access_service.utils.date_time_utils import (
        end_of_day_nano,
        ensure_timezone,
        start_of_day_nano,
    )

    # convert into utc:
    requested_start_date = ensure_timezone(requested_start_date)
    requested_end_date = ensure_timezone(requested_end_date)

    # throw error if requested start date is after requested end date
    if requested_start_date > requested_end_date:
        raise ValueError(
            f"Requested start date {requested_start_date} is after requested end date {requested_end_date}"
        )

    # seed values for the min/max scan below: any real extent starts before
    # "now" and ends after the epoch. now() must be evaluated per call - do
    # not move it into a constant, it would be frozen at server start.
    min_date_of_keys = pd.Timestamp.now(tz="UTC")
    max_date_of_keys = UNIX_EPOCH_UTC

    # get the union temporal extents of all selected keys
    got_any_extent = False
    for key in keys:
        try:
            start_date, end_date = api.get_temporal_extent(uuid, key)
        except KeyError:
            # key does not exist in this dataset; callers report/skip unknown
            # keys downstream, they must not break the trim
            continue
        if start_date is None or end_date is None:
            # if didn't get the temporal extent (e.g. when testing) just return the requested dates
            return requested_start_date, requested_end_date
        start_date = ensure_timezone(start_date)
        end_date = ensure_timezone(end_date)
        if start_date < min_date_of_keys:
            min_date_of_keys = start_date
        if end_date > max_date_of_keys:
            max_date_of_keys = end_date
        got_any_extent = True

    if not got_any_extent:
        # no key could report an extent; nothing to trim against
        return requested_start_date, requested_end_date

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

    # keep full-day semantics: floor the start and ceil the end to the
    # first/final nanosecond of their days
    return start_of_day_nano(trimmed_start_date), end_of_day_nano(trimmed_end_date)
