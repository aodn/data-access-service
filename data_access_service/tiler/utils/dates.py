"""Pure date utility functions shared across routers and services."""

from zoneinfo import ZoneInfo

import pandas as pd

from data_access_service.config.config import Config

# All date strings exposed by the API are local dates in this timezone.
# Requests must send them back unchanged.
LOCAL_TZ = ZoneInfo(Config.get_config().get_tiler_config().tile_timezone)


def ts_to_local_date(ts) -> str:
    """Convert a UTC numpy datetime64 or Timestamp to a local date string (YYYY-MM-DD)."""
    return str(
        pd.Timestamp(ts).tz_localize("UTC").tz_convert(LOCAL_TZ).strftime("%Y-%m-%d")
    )


def ts_to_local_rfc3339(ts) -> str:
    """Convert a UTC numpy datetime64 or Timestamp to a local RFC 3339 instant
    (e.g. 2026-07-16T00:00:00+10:00) — the external form of a tile date in
    OGC coverage discovery. Its local calendar date is exactly ts_to_local_date."""
    return pd.Timestamp(ts).tz_localize("UTC").tz_convert(LOCAL_TZ).isoformat()
