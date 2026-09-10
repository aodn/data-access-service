import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pytest

from data_access_service.batch.subsetting.helpers.time_column import (
    STRING_TIME_FORMAT,
    TimeColumn,
    resolve_time_column,
)


def _dataset(column_name: str, values: list, arrow_type: pa.DataType) -> pds.Dataset:
    table = pa.table({column_name: pa.array(values, type=arrow_type)})
    return pds.dataset(table)


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
