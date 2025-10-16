import pytest
import pandas as pd
from fastapi import HTTPException
from data_access_service.utils.routes_helper import verify_datatime_param


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
