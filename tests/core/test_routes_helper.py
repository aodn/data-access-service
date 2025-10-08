import pytest
import pandas as pd
import pytz

from data_access_service.utils.routes_helper import _verify_datatime_param
from fastapi import HTTPException


def test_valid_datetime_with_nanoseconds():
    dt = "2024-06-01 12:34:56.123456789"
    result = _verify_datatime_param("test", dt)
    assert isinstance(result, pd.Timestamp)
    assert result == pd.Timestamp(dt).tz_localize(pytz.UTC)


def test_missing_time_component():
    dt = "2024-06-01"
    with pytest.raises(HTTPException) as exc:
        _verify_datatime_param("test", dt)
    assert "Time with nanosecond precision missing" in str(exc.value.detail)


def test_missing_nanosecond_precision():
    dt = "2024-06-01 12:34:56.123"
    with pytest.raises(HTTPException) as exc:
        _verify_datatime_param("test", dt)
    assert "Time with nanosecond precision missing" in str(exc.value.detail)


def test_none_input():
    result = _verify_datatime_param("test", None)
    assert result is None
