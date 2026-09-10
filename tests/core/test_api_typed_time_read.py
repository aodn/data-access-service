"""The parquet read used when the time column is not a timestamp (issue 9144)."""

import tempfile
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
from types import SimpleNamespace

from data_access_service.core.api import API
from data_access_service.utils.time_column_utils import (
    STRING_TIME_FORMAT,
    resolve_time_column,
)


def _datasource() -> SimpleNamespace:
    """Stands in for a ParquetDataSource: the read only needs `.dataset`."""
    days = pd.date_range("2010-08-01", "2010-09-30", freq="D")
    table = pa.table(
        {
            "date_hour_UTC": pa.array(
                # Reverse order, so a sorted result can only come from the sort.
                [day.strftime(STRING_TIME_FORMAT) for day in days[::-1]],
                type=pa.string(),
            ),
            "station_name": pa.array(
                ["a" if i % 2 else "b" for i in range(len(days))], type=pa.string()
            ),
            "timestamp": pa.array(
                [
                    int(pd.Timestamp(day.year, day.month, 1).timestamp())
                    for day in days[::-1]
                ],
                type=pa.int32(),
            ),
        }
    )
    path = tempfile.mkdtemp()
    pq.write_to_dataset(table, path, partition_cols=["timestamp"])
    return SimpleNamespace(dataset=pds.dataset(path, partitioning="hive"))


class TestReadParquetWithTypedTime(unittest.TestCase):
    def _read(self, columns=None, scalar_filter=None) -> pd.DataFrame:
        ds = _datasource()
        column = resolve_time_column(ds.dataset, "date_hour_UTC")
        # Unbound call: the method needs no instance state.
        return API._read_parquet_with_typed_time(
            None,
            ds,
            column,
            pd.Timestamp("2010-08-10"),
            pd.Timestamp("2010-08-14 23:59:59.999999999"),
            None,
            None,
            None,
            None,
            None,
            None,
            scalar_filter,
            columns,
        )

    def test_keeps_only_the_requested_range(self):
        df = self._read()

        self.assertEqual(len(df), 5)
        self.assertEqual(
            df["date_hour_UTC"].iloc[0],
            pd.Timestamp("2010-08-10").strftime(STRING_TIME_FORMAT),
        )

    def test_result_is_sorted_by_time(self):
        df = self._read()

        self.assertTrue(df["date_hour_UTC"].is_monotonic_increasing)
        self.assertEqual(list(df.index), list(range(len(df))))

    def test_column_subset_without_the_time_column(self):
        df = self._read(columns=["station_name"])

        self.assertEqual(list(df.columns), ["station_name"])
        self.assertEqual(len(df), 5)

    def test_scalar_filter(self):
        df = self._read(scalar_filter={"station_name": "a"})

        self.assertEqual(set(df["station_name"]), {"a"})
        self.assertLess(len(df), 5)
