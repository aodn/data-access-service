"""Pure date utility functions shared across routers and services.
"""

import pandas as pd


def ts_to_utc_iso(ts) -> str:
    """Format a raw (naive UTC) store timestamp as a canonical UTC ISO-8601 string."""
    return pd.Timestamp(ts).tz_localize("UTC").isoformat().replace("+00:00", "Z")


def parse_query_date(date: str) -> pd.Timestamp:
    """Parse a client-supplied date/timestamp string into a naive-UTC instant."""
    ts = pd.Timestamp(date)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts
