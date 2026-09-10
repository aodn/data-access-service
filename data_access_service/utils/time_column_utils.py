"""Resolve a dataset's time column to something a pyarrow filter can compare against.

A parquet filter is pushed down into the scan, so the literal on the right of the
comparison must match the column type stored on disk -- pyarrow has no kernel for
mixed pairs such as ``(string, timestamp[s])`` and raises at scan time. Converting
the column instead would mean reading it in full, which defeats the pushdown.

So the conversion goes the other way: the rest of the pipeline keeps working in
``pd.Timestamp``, and only :meth:`TimeColumn.to_literal` knows the stored type.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import compute as pc

from aodn_cloud_optimised.lib.DataQuery import get_timestamps_boundary_values

from data_access_service.utils.date_time_utils import to_naive_utc_string

log = logging.getLogger(__name__)

# Layout of a string time column, e.g. "2007-08-08 07:00:00.000000Z". Fixed width
# and zero padded, so lexicographic order matches chronological order -- that is
# what makes a plain string comparison a valid time filter.
STRING_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%fZ"


@dataclass(frozen=True)
class TimeColumn:
    """A dataset's time column plus how to write a literal for it."""

    name: str
    arrow_type: pa.DataType

    @property
    def is_timestamp(self) -> bool:
        """True when the library's own filter builder can handle this column."""
        return pa.types.is_timestamp(self.arrow_type)

    @property
    def is_string(self) -> bool:
        return pa.types.is_string(self.arrow_type) or pa.types.is_large_string(
            self.arrow_type
        )

    def to_literal(self, value: pd.Timestamp) -> Any:
        """A pd.Timestamp rendered as a literal comparable with this column."""
        naive = (
            value.tz_convert("UTC").tz_localize(None) if value.tz is not None else value
        )
        if pa.types.is_timestamp(self.arrow_type):
            # Match precision and timezone exactly, otherwise e.g. a timestamp[s]
            # literal against a timestamp[ns] column has no kernel.
            return pa.scalar(naive, type=self.arrow_type)
        if self.is_string:
            return naive.strftime(STRING_TIME_FORMAT)
        if pa.types.is_integer(self.arrow_type):
            # The hive partition key, holding unix seconds. It is rejected during
            # column mapping, so reaching here means that guard was bypassed.
            # TypeError, not ValueError: callers treat ValueError as "no overlap"
            # and skip the range, which would drop data silently (issue 9144).
            raise TypeError(
                f"Time column {self.name!r} is {self.arrow_type}, which looks like "
                f"a hive partition key rather than a time column."
            )
        # date32 and friends already compare correctly against a pd.Timestamp.
        return naive


def resolve_time_column(dataset: ds.Dataset, name: str) -> TimeColumn:
    """Build the :class:`TimeColumn` for `name`, validating a string column's layout.

    A string column is only safe to compare lexicographically when every value
    uses the same fixed-width layout. A mismatch would filter silently and
    wrongly, so fail loudly here instead.
    """
    arrow_type = dataset.schema.field(name).type
    column = TimeColumn(name=name, arrow_type=arrow_type)

    if column.is_string:
        sample = _first_value(dataset, name)
        if sample is None:
            raise ValueError(
                f"Time column {name!r} is a string column but has no value to "
                f"validate its format against."
            )
        try:
            datetime.strptime(sample, STRING_TIME_FORMAT)
        except ValueError as e:
            raise ValueError(
                f"Time column {name!r} holds {sample!r}, which does not match "
                f"{STRING_TIME_FORMAT!r}. Comparing it as a string would filter "
                f"on the wrong order."
            ) from e
        expected_width = len(
            pd.Timestamp("2000-01-01 00:00:00").strftime(STRING_TIME_FORMAT)
        )
        if len(sample) != expected_width:
            raise ValueError(
                f"Time column {name!r} holds {sample!r} of width {len(sample)}, "
                f"expected {expected_width}. Values must be fixed width for "
                f"lexicographic comparison to match time order."
            )
        log.info(
            "Time column %s is a string column matching %s", name, STRING_TIME_FORMAT
        )

    return column


def _first_value(dataset: ds.Dataset, name: str) -> str | None:
    """First non-null value of `name`, or None when the column is empty."""
    for batch in dataset.scanner(columns=[name], batch_size=1024).to_batches():
        for value in batch.column(0).to_pylist():
            if value is not None:
                return value
    return None


def partition_timestamp_scalar(dataset: ds.Dataset, value) -> pa.Scalar:
    """Cast a `timestamp` partition boundary to that field's own type.

    Hive partition types are inferred from directory names, so the same value is
    int32 in one dataset and a string in another, and a mismatched literal
    raises the same ArrowNotImplementedError this module exists to avoid.
    """
    try:
        ts_type = dataset.schema.field("timestamp").type
    except KeyError:
        ts_type = pa.int64()

    if pa.types.is_string(ts_type) or pa.types.is_large_string(ts_type):
        return pa.scalar(str(int(value)), type=ts_type)
    return pa.scalar(int(value), type=ts_type)


def build_time_filter(
    dataset: ds.Dataset,
    time_column: TimeColumn,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pc.Expression:
    """Filter for `start`..`end` (both inclusive) on `time_column`.

    Same shape as the library's ``create_time_filter``: prune the `timestamp`
    partitions first, then compare the real time column row by row. The
    difference is the literal, which comes from :meth:`TimeColumn.to_literal`
    and so matches the stored type.
    """
    expression = pc.field(time_column.name) >= time_column.to_literal(start)
    expression = expression & (
        pc.field(time_column.name) <= time_column.to_literal(end)
    )

    if "timestamp" not in dataset.schema.names:
        # Not partitioned by time, so the row-level comparison is all there is.
        return expression

    partition_start, partition_end = get_timestamps_boundary_values(
        dataset, to_naive_utc_string(start), to_naive_utc_string(end)
    )
    return (
        (pc.field("timestamp") >= partition_timestamp_scalar(dataset, partition_start))
        & (pc.field("timestamp") <= partition_timestamp_scalar(dataset, partition_end))
        & expression
    )
