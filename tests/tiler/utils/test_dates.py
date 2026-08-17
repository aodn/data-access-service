"""UTC date/timestamp helpers — the tiler addresses data by exact instant, not
by calendar day. These tests pin the conversion both directions: a store
timestamp must format to the exact string a client can parse back to the
same instant.
"""

import numpy as np
import pandas as pd
import pytest

from data_access_service.tiler.utils import dates as dates_mod


def test_ts_to_utc_iso_formats_naive_store_timestamp():
    ts = pd.Timestamp("2024-06-15T23:00:00")
    assert dates_mod.ts_to_utc_iso(ts) == "2024-06-15T23:00:00Z"


def test_ts_to_utc_iso_handles_numpy_datetime64():
    """The function is called with numpy datetime64 from Zarr — must accept it."""
    ts = np.datetime64("2024-01-01T00:00:00")
    assert dates_mod.ts_to_utc_iso(ts) == "2024-01-01T00:00:00Z"


def test_parse_query_date_accepts_bare_date():
    assert dates_mod.parse_query_date("2024-06-15") == pd.Timestamp(
        "2024-06-15T00:00:00"
    )


def test_parse_query_date_accepts_full_timestamp_with_z():
    assert dates_mod.parse_query_date("2024-06-15T23:00:00Z") == pd.Timestamp(
        "2024-06-15T23:00:00"
    )


def test_parse_query_date_accepts_full_timestamp_with_offset():
    assert dates_mod.parse_query_date("2024-06-15T23:00:00+00:00") == pd.Timestamp(
        "2024-06-15T23:00:00"
    )


def test_parse_query_date_converts_non_utc_offset_to_utc():
    # 2024-06-16T09:00:00+10:00 is the same instant as 2024-06-15T23:00:00Z.
    assert dates_mod.parse_query_date("2024-06-16T09:00:00+10:00") == pd.Timestamp(
        "2024-06-15T23:00:00"
    )


def test_parse_query_date_rejects_unparseable_string():
    with pytest.raises(ValueError):
        dates_mod.parse_query_date("not-a-date")


def test_round_trip_through_ts_to_utc_iso_and_parse_query_date():
    """A client parsing back what the server formatted must land on the exact
    same instant — this is the invariant the whole exact-instant lookup relies on."""
    raw = np.datetime64("2022-05-31T15:20:00")
    formatted = dates_mod.ts_to_utc_iso(raw)
    assert dates_mod.parse_query_date(formatted) == pd.Timestamp(raw)
