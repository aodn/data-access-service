"""Pure date utility functions shared across routers and services.
"""

import pandas as pd


def ts_to_utc_iso(ts) -> str:
    """Format a raw (naive UTC) store timestamp as a canonical UTC ISO-8601 string."""
    return pd.Timestamp(ts).tz_localize("UTC").isoformat().replace("+00:00", "Z")


def str_to_utc_timestamp(date: str) -> pd.Timestamp:
    """Parse a date/timestamp string (any offset, or none) into a naive-UTC timestamp.

    Any valid spelling of the same instant collapses to the same value —
    e.g. '...Z' and '...+00:00' parse equal (and hash equal), so callers can
    compare/look up by the returned pd.Timestamp without caring which offset
    notation the client used.
    """
    ts = pd.Timestamp(date)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts
