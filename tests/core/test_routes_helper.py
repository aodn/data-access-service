import pytest
import pandas as pd
from fastapi import HTTPException
from data_access_service.utils.routes_helper import (
    verify_datatime_param,
    parse_utc_datetime,
)


def test_valid_datetime_with_timezone():
    result = verify_datatime_param("start_date", "2023-01-01T12:00:00+00:00")
    assert isinstance(result, pd.Timestamp)
    assert result.tz is not None


def test_valid_datetime_without_timezone():
    result = verify_datatime_param("start_date", "2023-01-01T12:00:00")
    assert isinstance(result, pd.Timestamp)
    assert result.tz is not None  # Should be localized to UTC


def test_invalid_datetime_format():
    with pytest.raises(HTTPException) as excinfo:
        verify_datatime_param("start_date", "not-a-date")
    assert "Incorrect format" in str(excinfo.value.detail)


def test_end_date_requires_nanosecond_precision():
    with pytest.raises(HTTPException) as excinfo:
        verify_datatime_param("end_date", "2023-01-01T12:00:00.123456")
    assert "nanosecond precision missing" in str(excinfo.value.detail)


def test_end_date_with_nanosecond_precision():
    result = verify_datatime_param("end_date", "2023-01-01T12:00:00.123456789")
    assert isinstance(result, pd.Timestamp)
    assert result.tz is not None


def test_none_date_returns_none():
    result = verify_datatime_param("start_date", None)
    assert result is None


def test_non_end_date_does_not_require_nanosecond_precision():
    # Should not raise an exception even if nanosecond precision is missing
    result = verify_datatime_param("start_date", "2023-01-01")
    assert isinstance(result, pd.Timestamp)


def test_parse_utc_none_returns_none():
    assert parse_utc_datetime("start_date", None) is None


def test_parse_utc_naive_assumed_utc():
    # No offset given -> assumed UTC, returned without a tz suffix.
    assert (
        parse_utc_datetime("start_date", "2024-01-01T00:00:00") == "2024-01-01T00:00:00"
    )


def test_parse_utc_zulu_normalized():
    assert (
        parse_utc_datetime("start_date", "2024-01-01T00:00:00Z")
        == "2024-01-01T00:00:00"
    )


def test_parse_utc_offset_converted_to_utc():
    # +10:00 is 10 hours ahead, so the UTC instant is 10 hours earlier.
    assert (
        parse_utc_datetime("start_date", "2024-01-01T10:00:00+10:00")
        == "2024-01-01T00:00:00"
    )


def test_parse_utc_preserves_nanoseconds():
    assert (
        parse_utc_datetime("end_date", "2024-01-01T00:00:00.123456789")
        == "2024-01-01T00:00:00.123456789"
    )


def test_parse_utc_invalid_raises_400():
    with pytest.raises(HTTPException) as excinfo:
        parse_utc_datetime("start_date", "not-a-date")
    assert excinfo.value.status_code == 400
    assert "start_date" in str(excinfo.value.detail)
