"""Unit tests for estimation-index helpers used by download row-count splits."""

from unittest.mock import Mock, patch

import pandas as pd

from data_access_service.core.estimation_index import (
    count_index_rows,
    index_coverage_end,
    usable_sidecar,
)


def test_index_coverage_end_is_last_nanosecond_of_max_date():
    meta = Mock()
    meta.max_date = 20230115
    end = index_coverage_end(meta)
    expected = pd.Timestamp("2023-01-15 23:59:59.999999999", tz="UTC")
    assert end.value == expected.value


def test_count_index_rows_returns_none_on_query_error():
    meta = Mock()
    meta.has_time = True
    start = pd.Timestamp("2023-01-01", tz="UTC")
    end = pd.Timestamp("2023-01-31", tz="UTC")
    with patch(
        "data_access_service.core.estimation_index._get_client",
        side_effect=RuntimeError("s3 down"),
    ):
        assert count_index_rows("uuid", "key.parquet", meta, start, end) is None


def test_usable_sidecar_rejects_non_csv_format():
    assert usable_sidecar(Mock(), "uuid", "key.parquet", "netcdf") is None
