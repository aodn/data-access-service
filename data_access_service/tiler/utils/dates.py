"""Date helpers."""

import pandas as pd

from data_access_service.utils.date_time_utils import to_utc_iso_z


def ts_to_utc_iso(ts) -> str:
    """A naive-UTC timestamp as a UTC ISO-8601 string."""
    return to_utc_iso_z(ts)


def str_to_utc_timestamp(date: str, *, require_tz: bool = False) -> pd.Timestamp:
    """Parse ``date`` into a naive-UTC timestamp, so '...Z' and '...+00:00'
    compare equal. Input without an offset is taken as UTC, or rejected if
    ``require_tz``."""
    ts = pd.Timestamp(date)
    if ts.tzinfo is not None:
        return ts.tz_convert("UTC").tz_localize(None)
    if require_tz:
        raise ValueError(f"{date!r} has no UTC offset/designator")
    return ts
