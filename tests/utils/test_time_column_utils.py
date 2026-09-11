import tempfile
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest
from pyarrow import compute as pc

from data_access_service.utils.time_column_utils import (
    STRING_TIME_FORMAT,
    TimeColumn,
    build_time_filter,
    partition_timestamp_scalar,
    resolve_time_column,
    timestamp_partition_filter,
)


def _dataset(column_name: str, values: list, arrow_type: pa.DataType) -> pds.Dataset:
    table = pa.table({column_name: pa.array(values, type=arrow_type)})
    return pds.dataset(table)


def _string_time_dataset(partitioned: bool = True) -> pds.Dataset:
    """A daily dataset shaped like animal_acoustic_tracking: the time column is a
    fixed width string, and the partition key holds unix seconds as int32."""
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
    if not partitioned:
        return pds.dataset(table.drop_columns(["timestamp"]))

    path = tempfile.mkdtemp()
    pq.write_to_dataset(table, path, partition_cols=["timestamp"])
    return pds.dataset(path, partitioning="hive")


class TestTimeColumnLiteral(unittest.TestCase):
    def test_timestamp_column_keeps_column_precision(self):
        column = TimeColumn(name="TIME", arrow_type=pa.timestamp("ns"))
        literal = column.to_literal(pd.Timestamp("2025-01-01 12:00:00"))

        # A timestamp[s] literal against a timestamp[ns] column has no kernel.
        self.assertIsInstance(literal, pa.Scalar)
        self.assertEqual(literal.type, pa.timestamp("ns"))

    def test_timestamp_column_converts_aware_input_to_utc(self):
        column = TimeColumn(name="TIME", arrow_type=pa.timestamp("ns"))
        aware = pd.Timestamp("2025-01-01 12:00:00", tz="Australia/Hobart")

        self.assertEqual(
            column.to_literal(aware).as_py(),
            aware.tz_convert("UTC").tz_localize(None).to_pydatetime(),
        )

    def test_string_column_renders_the_stored_layout(self):
        column = TimeColumn(name="date_hour_UTC", arrow_type=pa.string())

        self.assertEqual(
            column.to_literal(pd.Timestamp("2010-01-01 19:00:00")),
            "2010-01-01 19:00:00.000000Z",
        )

    def test_integer_column_raises_type_error(self):
        column = TimeColumn(name="timestamp", arrow_type=pa.int32())

        # The hive partition key must never reach here. TypeError, not
        # ValueError: callers read ValueError as "no overlap" and skip the
        # range, which would drop data silently.
        with pytest.raises(TypeError, match="hive partition key"):
            column.to_literal(pd.Timestamp("2025-01-01"))

    def test_date32_column_keeps_the_plain_timestamp(self):
        # Seagrass filters on _temporal_extent, a date32 column. pyarrow already
        # has a (date32, timestamp) kernel, so it must not be altered.
        column = TimeColumn(name="_temporal_extent", arrow_type=pa.date32())

        self.assertEqual(
            column.to_literal(pd.Timestamp("2025-02-23")), pd.Timestamp("2025-02-23")
        )


class TestResolveTimeColumn(unittest.TestCase):
    def test_timestamp_column_needs_no_sample(self):
        dataset = _dataset("TIME", [pd.Timestamp("2025-01-01")], pa.timestamp("ns"))
        column = resolve_time_column(dataset, "TIME")

        self.assertFalse(column.is_string)
        self.assertEqual(column.arrow_type, pa.timestamp("ns"))

    def test_string_column_matching_the_layout_is_accepted(self):
        dataset = _dataset(
            "date_hour_UTC",
            ["2007-08-08 07:00:00.000000Z", "2010-01-01 19:00:00.000000Z"],
            pa.string(),
        )
        column = resolve_time_column(dataset, "date_hour_UTC")

        self.assertTrue(column.is_string)

    def test_string_column_with_another_layout_is_rejected(self):
        # Lexicographic order would not match time order, so filtering would be
        # silently wrong. Fail instead.
        dataset = _dataset("when", ["08/08/2007 07:00"], pa.string())

        with pytest.raises(ValueError):
            resolve_time_column(dataset, "when")

    def test_variable_width_string_column_is_rejected(self):
        dataset = _dataset("when", ["2007-8-8 7:00:00.0Z"], pa.string())

        with pytest.raises(ValueError):
            resolve_time_column(dataset, "when")

    def test_empty_string_column_is_rejected(self):
        dataset = _dataset("when", [None], pa.string())

        with pytest.raises(ValueError, match="no value to validate"):
            resolve_time_column(dataset, "when")

    def test_round_trip_over_the_supported_range(self):
        column = TimeColumn(name="date_hour_UTC", arrow_type=pa.string())
        for stamp in ("1970-01-01 00:00:00", "2026-12-05 22:00:00"):
            rendered = column.to_literal(pd.Timestamp(stamp))
            self.assertEqual(len(rendered), len("2007-08-08 07:00:00.000000Z"))
            self.assertEqual(
                pd.Timestamp(rendered).tz_localize(None), pd.Timestamp(stamp)
            )
        self.assertTrue(STRING_TIME_FORMAT.endswith("Z"))


class TestBuildTimeFilter(unittest.TestCase):
    def _rows(self, dataset: pds.Dataset, start: str, end: str) -> list:
        column = resolve_time_column(dataset, "date_hour_UTC")
        time_filter = build_time_filter(
            dataset, column, pd.Timestamp(start), pd.Timestamp(end)
        )
        return (
            dataset.to_table(filter=time_filter).to_pandas()["date_hour_UTC"].tolist()
        )

    def test_string_column_keeps_the_requested_range(self):
        rows = self._rows(
            _string_time_dataset(), "2010-08-05", "2010-08-10 23:59:59.999999999"
        )

        self.assertEqual(len(rows), 6)
        self.assertEqual(
            rows[0], pd.Timestamp("2010-08-05").strftime(STRING_TIME_FORMAT)
        )
        self.assertEqual(
            rows[-1], pd.Timestamp("2010-08-10").strftime(STRING_TIME_FORMAT)
        )

    def test_range_crossing_a_partition_boundary(self):
        # The partition key only changes once a month, so a range spanning two
        # months must keep both partitions.
        rows = self._rows(
            _string_time_dataset(), "2010-08-30", "2010-09-02 23:59:59.999999999"
        )

        self.assertEqual(len(rows), 4)

    def test_dataset_without_a_timestamp_partition(self):
        rows = self._rows(
            _string_time_dataset(partitioned=False),
            "2010-08-05",
            "2010-08-10 23:59:59.999999999",
        )

        self.assertEqual(len(rows), 6)

    def test_library_style_literal_is_what_fails(self):
        # The bug this filter works around (issue 9144): comparing the same
        # string column against a pd.Timestamp has no pyarrow kernel.
        dataset = _string_time_dataset()

        with pytest.raises(pa.lib.ArrowNotImplementedError):
            dataset.to_table(
                filter=pc.field("date_hour_UTC") >= pd.to_datetime("2010-08-05")
            )


class TestTimestampPartitionFilter(unittest.TestCase):
    def test_none_when_dataset_is_not_partitioned_by_timestamp(self):
        dataset = _string_time_dataset(partitioned=False)

        self.assertIsNone(
            timestamp_partition_filter(
                dataset, pd.Timestamp("2010-08-05"), pd.Timestamp("2010-08-10")
            )
        )

    def test_prunes_to_the_covering_timestamp_buckets(self):
        dataset = _string_time_dataset()
        start = pd.Timestamp("2010-08-05")
        end = pd.Timestamp("2010-08-10 23:59:59.999999999")

        expr = timestamp_partition_filter(dataset, start, end)

        self.assertIsNotNone(expr)
        rows = dataset.to_table(filter=expr).to_pandas()
        august = int(pd.Timestamp("2010-08-01").timestamp())
        self.assertTrue((rows["timestamp"] == august).all())
        self.assertGreater(len(rows), 0)


class TestPartitionTimestampScalar(unittest.TestCase):
    def test_follows_the_partition_field_type(self):
        dataset = _string_time_dataset()

        scalar = partition_timestamp_scalar(dataset, 1280620800)

        self.assertEqual(scalar.type, pa.int32())

    def test_string_partition_gets_a_string_literal(self):
        dataset = _dataset("timestamp", ["1280620800"], pa.string())

        self.assertEqual(
            partition_timestamp_scalar(dataset, 1280620800).as_py(), "1280620800"
        )

    def test_missing_partition_field_falls_back_to_int64(self):
        dataset = _dataset("when", ["2010-08-01 00:00:00.000000Z"], pa.string())

        self.assertEqual(
            partition_timestamp_scalar(dataset, 1280620800).type, pa.int64()
        )
