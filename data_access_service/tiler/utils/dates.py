import pandas as pd

from data_access_service.utils.date_time_utils import to_utc_iso_z

_COMPACT = "%Y%m%dT%H%M%SZ"


def ts_to_utc_iso(ts) -> str:
    return to_utc_iso_z(pd.Timestamp(ts))


def str_to_utc_timestamp(date: str, *, require_tz: bool = False) -> pd.Timestamp:
    ts = pd.Timestamp(date)
    if ts.tzinfo is not None:
        return ts.tz_convert("UTC").tz_localize(None)
    if require_tz:
        raise ValueError(f"{date!r} has no UTC offset/designator")
    return ts


def compact_timestamp(date: str | pd.Timestamp) -> str:
    ts = date if isinstance(date, pd.Timestamp) else str_to_utc_timestamp(str(date))
    return ts.strftime(_COMPACT)


def iso_timestamp(compact_or_ts: str | pd.Timestamp) -> str:
    if isinstance(compact_or_ts, pd.Timestamp):
        return to_utc_iso_z(compact_or_ts)
    try:
        ts = pd.to_datetime(compact_or_ts, format=_COMPACT)
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        return to_utc_iso_z(ts)
    except (ValueError, TypeError):
        return to_utc_iso_z(str_to_utc_timestamp(compact_or_ts))
