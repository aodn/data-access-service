"""check_rows_with_date_range: timestamp partition pruning vs row-level counts."""

import tempfile
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq

from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

from data_access_service.batch.subsetting.helpers import parquet_date_ranges as pdr
from data_access_service.batch.subsetting.helpers.parquet_date_ranges import (
    check_rows_with_date_range,
)
from data_access_service.utils.time_column_utils import (
    STRING_TIME_FORMAT,
    timestamp_partition_filter,
)


def _datasource(dataset, dname="data.parquet"):
    source = MagicMock(spec=ParquetDataSource)
    source.dataset = dataset
    source.dname = dname
    return source


def _api(time_name: str) -> Mock:
    api = Mock()
    api.require_time_column.return_value = time_name
    return api


def _range(start: str, end: str) -> dict:
    return {
        "start_date": pd.Timestamp(start, tz="UTC"),
        "end_date": pd.Timestamp(end, tz="UTC"),
    }


def _hive_dataset(times: list[pd.Timestamp], partition_month: str = "2023-01-01"):
    """One hive `timestamp` bucket holding `times` as a TIME column."""
    bucket = int(pd.Timestamp(partition_month).timestamp())
    table = pa.table(
        {
            "TIME": pa.array(times, type=pa.timestamp("ns")),
            "timestamp": pa.array([bucket] * len(times), type=pa.int32()),
        }
    )
    path = tempfile.mkdtemp()
    pq.write_to_dataset(table, path, partition_cols=["timestamp"])
    return pds.dataset(path, partitioning="hive")


def _unpartitioned_dataset(times: list[pd.Timestamp]):
    table = pa.table({"TIME": pa.array(times, type=pa.timestamp("ns"))})
    return pds.dataset(table)


def _string_time_hive_dataset():
    days = pd.date_range("2010-08-01", "2010-10-01", freq="D")
    table = pa.table(
        {
            "date_hour_UTC": pa.array(
                [day.strftime(STRING_TIME_FORMAT) for day in days], type=pa.string()
            ),
            "timestamp": pa.array(
                [int(pd.Timestamp(day.year, day.month, 1).timestamp()) for day in days],
                type=pa.int32(),
            ),
        }
    )
    path = tempfile.mkdtemp()
    pq.write_to_dataset(table, path, partition_cols=["timestamp"])
    return pds.dataset(path, partitioning="hive")


def test_unpartitioned_dataset_counts_the_time_column():
    times = list(pd.date_range("2023-01-01", periods=8, freq="D"))
    dataset = _unpartitioned_dataset(times)

    result = check_rows_with_date_range(
        api=_api("TIME"),
        uuid="u",
        key="k",
        ds=_datasource(dataset),
        date_ranges=[_range("2023-01-03", "2023-01-05 23:59:59.999999999")],
    )

    assert len(result) == 1
    assert result[0]["start_date"] == pd.Timestamp("2023-01-03", tz="UTC")


def test_unpartitioned_dataset_skips_a_window_with_no_rows():
    times = list(pd.date_range("2023-01-01", periods=3, freq="D"))
    dataset = _unpartitioned_dataset(times)

    result = check_rows_with_date_range(
        api=_api("TIME"),
        uuid="u",
        key="k",
        ds=_datasource(dataset),
        date_ranges=[_range("2024-01-01", "2024-01-31")],
    )

    assert result == []


def test_timestamp_partition_keeps_overlapping_windows_without_counting_rows():
    times = list(pd.date_range("2023-01-01", periods=5, freq="D"))
    dataset = _hive_dataset(times)

    with patch.object(pdr, "_count_rows_with_retry") as count_rows:
        result = check_rows_with_date_range(
            api=_api("TIME"),
            uuid="u",
            key="k",
            ds=_datasource(dataset),
            date_ranges=[_range("2023-01-01", "2023-01-31")],
        )

    assert len(result) == 1
    count_rows.assert_not_called()


def test_timestamp_partition_does_not_split_on_row_counts():
    """Partition keys replace the row scan, so a large bucket stays one window."""
    times = list(pd.date_range("2023-01-01", periods=20, freq="D"))
    dataset = _hive_dataset(times)

    result = check_rows_with_date_range(
        api=_api("TIME"),
        uuid="u",
        key="k",
        ds=_datasource(dataset),
        date_ranges=[_range("2023-01-01", "2023-01-20 23:59:59.999999999")],
    )

    assert len(result) == 1
    assert result[0]["start_date"] == pd.Timestamp("2023-01-01", tz="UTC")


def test_string_time_column_with_timestamp_partition():
    dataset = _string_time_hive_dataset()

    result = check_rows_with_date_range(
        api=_api("date_hour_UTC"),
        uuid="u",
        key="k",
        ds=_datasource(dataset),
        date_ranges=[_range("2010-08-05", "2010-08-10 23:59:59.999999999")],
    )

    assert len(result) == 1


def test_timestamp_partition_skips_a_window_before_the_first_bucket():
    times = list(pd.date_range("2023-01-01", "2023-01-31", freq="D"))
    dataset = _hive_dataset(times)

    result = check_rows_with_date_range(
        api=_api("TIME"),
        uuid="u",
        key="k",
        ds=_datasource(dataset),
        date_ranges=[_range("2022-12-01", "2022-12-31")],
    )

    assert result == []


def test_argo_canned_keeps_a_window_inside_a_timestamp_bucket():
    dataset = pds.dataset(
        "tests/canned/s3_sample_edge_cases/argo.parquet",
        format="parquet",
        partitioning="hive",
    )

    result = check_rows_with_date_range(
        api=_api("JULD"),
        uuid="u",
        key="k",
        ds=_datasource(dataset),
        date_ranges=[_range("2003-01-01", "2003-01-31")],
    )

    assert len(result) == 1


def test_partition_filter_expression_does_not_mention_the_time_column():
    times = list(pd.date_range("2023-01-01", periods=5, freq="D"))
    dataset = _hive_dataset(times)

    expr = timestamp_partition_filter(
        dataset, pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-31")
    )

    assert expr is not None
    text = str(expr)
    assert "timestamp" in text
    assert "TIME" not in text
