"""Subset parquet writes stay one batch at a time."""

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds
import pyarrow.parquet as pq
import pytest

from shapely.geometry import box

from data_access_service.utils.parquet_filter import scan_parquet_batches
from data_access_service.core.constants import PARTITION_KEY
from data_access_service.batch.subsetting.tasks.parquet_processor import (
    _stream_window_to_parquet,
    _write_batches,
)


def _batch(rows, start):
    times = pd.date_range(start, periods=rows, freq="h")
    return pa.record_batch(
        {
            "TIME": pa.array(times),
            "LATITUDE": pa.array([1.0] * rows),
            "LONGITUDE": pa.array([2.0] * rows),
            "value": pa.array(range(rows), type=pa.int64()),
        }
    )


def test_scan_yields_bounded_batches(tmp_path):
    rows = 1000
    table = pa.table(
        {
            "TIME": pa.array(pd.date_range("2013-08-01", periods=rows, freq="h")),
            "value": pa.array(range(rows), type=pa.int64()),
        }
    )
    pds.write_dataset(table, tmp_path, format="parquet")
    dataset = pds.dataset(tmp_path)

    batches = list(
        scan_parquet_batches(
            dataset,
            pc.field("value") < pa.scalar(250, type=pa.int64()),
            None,
            100,
        )
    )

    assert batches
    assert max(batch.num_rows for batch in batches) <= 100
    assert sum(batch.num_rows for batch in batches) == 250


def test_write_batches_splits_months_and_does_not_overwrite(tmp_path):
    output = tmp_path / "out"
    mixed = pa.record_batch(
        {
            "TIME": pa.array(
                [
                    pd.Timestamp("2013-07-31 12:00:00"),
                    pd.Timestamp("2013-08-01 00:00:00"),
                ]
            ),
            "LATITUDE": pa.array([1.0, 1.0]),
            "LONGITUDE": pa.array([2.0, 2.0]),
            "value": pa.array([7, 8], type=pa.int64()),
        }
    )
    assert _write_batches([mixed], str(output), None, "TIME", None, None, None)

    july = pd.read_parquet(output / f"{PARTITION_KEY}=2013-07")
    august = pd.read_parquet(output / f"{PARTITION_KEY}=2013-08")
    assert list(july["value"]) == [7]
    assert list(august["value"]) == [8]
    assert PARTITION_KEY not in july.columns

    assert _write_batches(
        [_batch(2, "2013-08-02")], str(output), None, "TIME", None, None, None
    )
    parts = sorted((output / f"{PARTITION_KEY}=2013-08").glob("*.parquet"))
    assert [path.name for path in parts] == ["part.0.parquet", "part.1.parquet"]
    # The first August file is unchanged by the second window.
    assert pq.read_table(parts[0]).num_rows == 1


def test_write_batches_applies_the_polygon_and_uses_the_given_label(tmp_path):
    batch = pa.record_batch(
        {
            "TIME": pa.array([pd.Timestamp("2013-08-01"), pd.Timestamp("2013-08-02")]),
            "LATITUDE": pa.array([5.0, 50.0]),
            "LONGITUDE": pa.array([5.0, 5.0]),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    output = tmp_path / "poly"
    assert _write_batches(
        [batch],
        str(output),
        "polygon-0-1",
        None,
        box(0, 0, 10, 10),
        "LATITUDE",
        "LONGITUDE",
    )
    frame = pd.read_parquet(output / f"{PARTITION_KEY}=polygon-0-1")
    assert list(frame["value"]) == [1]
    assert "geometry" not in frame.columns


class _FakeApi:
    def __init__(self, batches=None, error=None):
        self.batches = batches or []
        self.error = error
        self.calls = []

    def iter_parquet_batches(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return iter(self.batches)


def test_stream_window_writes_without_get_dataset(tmp_path):
    api = _FakeApi([_batch(3, "2013-08-01")])
    output = tmp_path / "window"
    wrote = _stream_window_to_parquet(
        api,
        "uuid",
        "argo.parquet",
        pd.Timestamp("2013-08-01", tz="UTC"),
        pd.Timestamp("2013-08-31 23:59:59", tz="UTC"),
        -21.0,
        47.0,
        -43.0,
        49.0,
        str(output),
        time_key="TIME",
    )
    assert wrote
    assert api.calls[0]["key"] == "argo.parquet"
    assert not hasattr(api, "get_dataset") or "get_dataset" not in api.calls[0]
    frame = pd.read_parquet(output)
    assert len(frame) == 3


def test_stream_window_skips_a_date_outside_the_dataset(tmp_path):
    api = _FakeApi(
        error=ValueError(
            "date_start=2013-08-01 is out of range of dataset. "
            "The maximum date_end is 2013-07-01."
        )
    )
    assert (
        _stream_window_to_parquet(
            api,
            "uuid",
            "argo.parquet",
            pd.Timestamp("2013-08-01", tz="UTC"),
            pd.Timestamp("2013-08-31", tz="UTC"),
            None,
            None,
            None,
            None,
            str(tmp_path / "empty"),
            time_key="TIME",
        )
        is False
    )


def test_stream_window_reraises_other_value_errors():
    api = _FakeApi(error=ValueError("schema mismatch"))
    with pytest.raises(ValueError, match="schema mismatch"):
        _stream_window_to_parquet(
            api,
            "uuid",
            "argo.parquet",
            pd.Timestamp("2013-08-01", tz="UTC"),
            pd.Timestamp("2013-08-31", tz="UTC"),
            None,
            None,
            None,
            None,
            "/tmp/unused",
            time_key="TIME",
        )
