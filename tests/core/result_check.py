"""Compare a TestWithS3 result to the canned source it was read from.

Row order, dask wrappers, and CSV date strings are ignored. A change that
drops, duplicates, or alters a value fails the comparison.
"""

from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as pa_ds
import xarray
from pandas.testing import assert_frame_equal


def load_canned_parquet(folder: Path) -> pd.DataFrame:
    dataset = pa_ds.dataset(
        str(folder),
        format="parquet",
        partitioning="hive",
        exclude_invalid_files=True,
    )
    return dataset.to_table().to_pandas()


def rows_between(
    frame: pd.DataFrame, column: str, start: str, end: str
) -> pd.DataFrame:
    """Rows whose `column` falls inside an inclusive UTC window."""
    times = pd.to_datetime(frame[column], utc=True)
    window_start = pd.Timestamp(start, tz="UTC")
    window_end = pd.Timestamp(end, tz="UTC")
    return frame.loc[(times >= window_start) & (times <= window_end)].copy()


def _as_pandas(frame: pd.DataFrame) -> pd.DataFrame:
    compute = getattr(frame, "compute", None)
    if compute is not None and not isinstance(frame, pd.DataFrame):
        return compute()
    return frame


def _normalize_column(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        converted = pd.to_datetime(series, utc=True)
        return converted.dt.tz_convert("UTC").dt.tz_localize(None)
    if pd.api.types.is_numeric_dtype(series):
        return series
    if series.dtype == object or pd.api.types.is_string_dtype(series):
        present = int(series.notna().sum())
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
        if present and int(parsed.notna().sum()) >= int(0.8 * present):
            return parsed.dt.tz_convert("UTC").dt.tz_localize(None)
    return series


def assert_same_rows(
    actual: pd.DataFrame, expected: pd.DataFrame, columns: list[str]
) -> None:
    """The result carries the same values as `expected` for `columns`."""
    actual = _as_pandas(actual)
    expected = _as_pandas(expected)
    missing = [column for column in columns if column not in actual.columns]
    assert (
        not missing
    ), f"result is missing {missing}; columns are {list(actual.columns)}"
    missing_source = [column for column in columns if column not in expected.columns]
    assert not missing_source, f"canned source is missing {missing_source}"

    left = actual.loc[:, columns].copy()
    right = expected.loc[:, columns].copy()
    for column in columns:
        left[column] = _normalize_column(left[column])
        right[column] = _normalize_column(right[column])
    left = left.sort_values(columns, kind="mergesort", na_position="last").reset_index(
        drop=True
    )
    right = right.sort_values(
        columns, kind="mergesort", na_position="last"
    ).reset_index(drop=True)
    assert_frame_equal(
        left, right, check_dtype=False, check_exact=False, rtol=1e-5, atol=1e-5
    )


def assert_json_points_match(
    records: list[dict],
    source: pd.DataFrame,
    time_column: str,
    latitude_column: str,
    longitude_column: str,
) -> None:
    """JSON points match the source after the API's date cut and 1-decimal rounding."""
    times = pd.to_datetime(source[time_column], utc=True).dt.strftime("%Y-%m-%d")
    expected = Counter(
        zip(
            times,
            source[latitude_column].map(lambda value: round(float(value), 1)),
            source[longitude_column].map(lambda value: round(float(value), 1)),
        )
    )
    actual = Counter(
        (row["time"], row["latitude"], row["longitude"]) for row in records
    )
    if actual == expected:
        return
    missing = list((expected - actual).elements())[:5]
    extra = list((actual - expected).elements())[:5]
    raise AssertionError(
        f"JSON points do not match the canned source: "
        f"{sum(expected.values())} expected, {sum(actual.values())} returned; "
        f"missing sample {missing}; extra sample {extra}"
    )


def assert_exported_matches_source(
    exported: xarray.Dataset, source: xarray.Dataset, variables: list[str]
) -> None:
    """Each exported variable equals the canned store on the exported coordinates."""
    for name in variables:
        assert name in exported, f"{name} missing from export"
        assert name in source, f"{name} missing from canned source"
        left = exported[name]
        right = source[name]
        if left.dims != right.dims or left.shape != right.shape:
            right = right.sel({dim: exported[dim] for dim in left.dims})
        left_values = np.asarray(left.values)
        right_values = np.asarray(right.values)
        if np.issubdtype(left_values.dtype, np.number) or np.issubdtype(
            right_values.dtype, np.number
        ):
            np.testing.assert_allclose(
                left_values,
                right_values,
                equal_nan=True,
                err_msg=f"{name} does not match the canned source",
            )
        else:
            np.testing.assert_array_equal(
                left_values,
                right_values,
                err_msg=f"{name} does not match the canned source",
            )
