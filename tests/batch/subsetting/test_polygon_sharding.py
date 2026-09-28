"""A parquet dataset without a time column is split on its `polygon` partition
(polygon_ranges) instead of date windows; every other dataset keeps date_ranges.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import dask.dataframe as ddf
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pytest
from aodn_cloud_optimised.lib.DataQuery import ParquetDataSource

import data_access_service.batch.subsetting.subsetting_main as subsetting_main
from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.batch.subsetting.helpers.parquet_polygon_ranges import (
    split_polygon_ranges,
)
from data_access_service.batch.subsetting.tasks import parquet_processor
from data_access_service.core.constants import PARTITION_KEY
from data_access_service.models.subset_request import NON_SPECIFIED, SubsetRequest
from data_access_service.utils.subset_request_resolver import ResolvedSubsetRequest

UUID = "test-uuid"
KEY = "diver_site_information_qc.parquet"
POLYGON_COUNT = 7
ROWS_PER_POLYGON = 3


def _polygon_value(index: int) -> str:
    return f"0103000000010000000500000000{index:04X}" + "C0" * 78


def _write_polygon_only_dataset(root: Path) -> pd.DataFrame:
    rows = []
    for polygon_index in range(POLYGON_COUNT):
        for row in range(ROWS_PER_POLYGON):
            rows.append(
                {
                    "site_code": f"S{polygon_index}-{row}",
                    "latitude": -30.0 - polygon_index,
                    "longitude": 150.0 + row,
                    "polygon": _polygon_value(polygon_index),
                }
            )
    source = pd.DataFrame(rows)
    pq.write_to_dataset(
        pa.Table.from_pandas(source, preserve_index=False),
        root_path=str(root),
        partition_cols=["polygon"],
    )
    return source


def _subset_request(multi_polygon: str = NON_SPECIFIED) -> SubsetRequest:
    return SubsetRequest(
        uuid=UUID,
        keys=[KEY],
        start_date=NON_SPECIFIED,
        end_date=NON_SPECIFIED,
        recipient="user@example.com",
        multi_polygon=multi_polygon,
        output_format="csv",
    )


class FakeApi:
    """The API calls the polygon path makes, backed by a local hive dataset."""

    def __init__(self, root: Path, time_column=None, partitions=("polygon",)):
        self.dataset = pa_ds.dataset(str(root), format="parquet", partitioning="hive")
        self.datasource = MagicMock(spec=ParquetDataSource)
        self.datasource.dataset = self.dataset
        self.datasource.get_metadata.return_value = {}
        self.time_column = time_column
        self.partitions = frozenset(partitions)
        self.get_dataset_calls = []

    def resolve_dim_names(self, uuid, key):
        return "latitude", "longitude", self.time_column

    def get_partition_keys(self, uuid, key):
        return self.partitions

    def get_datasource(self, uuid, key):
        return self.datasource

    def map_column_names(self, uuid, key, columns):
        return ["latitude", "longitude"]

    def _filtered_frame(self, kwargs) -> pd.DataFrame:
        frame = self.dataset.to_table().to_pandas()
        frame["polygon"] = frame["polygon"].astype(str)
        for name, value in (kwargs.get("scalar_filter") or {}).items():
            frame = frame[frame[name] == value]
        if kwargs.get("lat_min") is not None:
            frame = frame[
                frame["latitude"].between(kwargs["lat_min"], kwargs["lat_max"])
                & frame["longitude"].between(kwargs["lon_min"], kwargs["lon_max"])
            ]
        return frame.reset_index(drop=True)

    def get_dataset(self, **kwargs):
        self.get_dataset_calls.append(kwargs)
        frame = self._filtered_frame(kwargs)
        if frame.empty:
            return None
        return ddf.from_pandas(frame, npartitions=1)

    def iter_parquet_batches(self, **kwargs):
        """What the subset writer scans now, one batch for the filtered rows."""
        self.get_dataset_calls.append(kwargs)
        frame = self._filtered_frame(kwargs)
        if frame.empty:
            return iter(())
        table = pa.Table.from_pandas(frame, preserve_index=False)
        return iter(table.to_batches())


def _run_init(monkeypatch, api, resolved_keys=(KEY,)) -> MagicMock:
    request = _subset_request()
    monkeypatch.setattr(
        subsetting_main, "get_subset_request", MagicMock(return_value=request)
    )
    monkeypatch.setattr(
        subsetting_main, "normalize_request", MagicMock(return_value=request)
    )
    monkeypatch.setattr(
        subsetting_main,
        "resolve_subset_request",
        MagicMock(
            return_value=ResolvedSubsetRequest(
                uuid=UUID,
                keys=list(resolved_keys),
                start_date=pd.Timestamp("2020-01-01", tz="UTC"),
                end_date=pd.Timestamp("2020-03-31", tz="UTC"),
                bboxes=[],
            )
        ),
    )
    monkeypatch.setattr(
        subsetting_main.Config,
        "get_polygon_count_per_job",
        staticmethod(lambda: 3),
    )
    aws_helper = MagicMock()
    monkeypatch.setattr(
        subsetting_main, "AWSHelper", MagicMock(return_value=aws_helper)
    )

    subsetting_main.init(api=api, job_id_of_init="job-1", parameters={})
    return aws_helper.submit_a_job


class TestInit:
    def test_dataset_without_time_column_is_split_by_polygon(
        self, monkeypatch, tmp_path
    ):
        _write_polygon_only_dataset(tmp_path)

        submit_a_job = _run_init(monkeypatch, FakeApi(tmp_path))

        preparation = submit_a_job.call_args_list[0].kwargs
        parameters = preparation["parameters"]
        assert Parameters.DATE_RANGES.value not in parameters
        polygon_ranges = json.loads(parameters[Parameters.POLYGON_RANGES.value])
        # 7 polygons at 3 per job
        assert len(polygon_ranges) == 3
        assert preparation["array_size"] == 3
        collection = submit_a_job.call_args_list[1].kwargs
        assert (
            collection["parameters"][Parameters.POLYGON_RANGES.value]
            == parameters[Parameters.POLYGON_RANGES.value]
        )

    def test_dataset_with_time_column_keeps_date_ranges(self, monkeypatch, tmp_path):
        _write_polygon_only_dataset(tmp_path)

        submit_a_job = _run_init(monkeypatch, FakeApi(tmp_path, time_column="TIME"))

        preparation = submit_a_job.call_args_list[0].kwargs
        parameters = preparation["parameters"]
        assert Parameters.POLYGON_RANGES.value not in parameters
        expected = subsetting_main.split_date_range(
            start_date=pd.Timestamp("2020-01-01", tz="UTC"),
            end_date=pd.Timestamp("2020-03-31", tz="UTC"),
            month_count_per_job=subsetting_main.Config.get_month_count_per_job(),
        )
        assert parameters[Parameters.DATE_RANGES.value] == json.dumps(expected)
        assert preparation["array_size"] == len(expected)

    def test_dataset_without_time_or_polygon_keeps_date_ranges(
        self, monkeypatch, tmp_path
    ):
        _write_polygon_only_dataset(tmp_path)

        submit_a_job = _run_init(monkeypatch, FakeApi(tmp_path, partitions=()))

        parameters = submit_a_job.call_args_list[0].kwargs["parameters"]
        assert Parameters.DATE_RANGES.value in parameters
        assert Parameters.POLYGON_RANGES.value not in parameters


class TestPrepareData:
    @pytest.fixture(autouse=True)
    def _no_upload(self, monkeypatch):
        monkeypatch.setattr(parquet_processor, "AWSHelper", MagicMock())
        monkeypatch.setattr(
            parquet_processor,
            "upload_all_files_in_folder_to_temp_s3",
            MagicMock(return_value="uploaded"),
        )

    @staticmethod
    def _prepare_all_children(api, output: Path, multi_polygon=NON_SPECIFIED):
        values = [_polygon_value(index) for index in range(POLYGON_COUNT)]
        polygon_ranges = split_polygon_ranges(values, polygon_count_per_job=3)
        request = _subset_request(multi_polygon)
        for job_index, polygon_range in polygon_ranges.items():
            parquet_processor.process_parquet_files(
                api,
                "job-1",
                job_index,
                str(output),
                request,
                pd.Timestamp("1970-01-01", tz="UTC"),
                pd.Timestamp("2026-01-01", tz="UTC"),
                polygon_range=polygon_range,
            )
        written = pd.read_parquet(output / KEY)
        return polygon_ranges, written

    def test_all_rows_written_once_across_children(self, tmp_path):
        source = _write_polygon_only_dataset(tmp_path / "source")
        api = FakeApi(tmp_path / "source")

        polygon_ranges, written = self._prepare_all_children(api, tmp_path / "out")

        assert len(written) == len(source)
        assert sorted(written["site_code"]) == sorted(source["site_code"])
        # one scan per polygon partition, none repeated
        filters = [call["scalar_filter"]["polygon"] for call in api.get_dataset_calls]
        assert sorted(filters) == sorted(set(source["polygon"]))
        # each child writes each polygon to its own partition directory
        for job_index in polygon_ranges:
            part = tmp_path / "out" / KEY / f"part-{job_index}"
            assert all(
                d.name.startswith(f"{PARTITION_KEY}=polygon-")
                for d in part.iterdir()
                if d.is_dir()
            )

    def test_area_filter_still_applies(self, tmp_path):
        _write_polygon_only_dataset(tmp_path / "source")
        api = FakeApi(tmp_path / "source")
        # Covers latitude -30.5..-32.5, i.e. polygons 1 and 2 only
        area = json.dumps(
            {
                "type": "MultiPolygon",
                "coordinates": [
                    [
                        [
                            [149, -32.5],
                            [153, -32.5],
                            [153, -30.5],
                            [149, -30.5],
                            [149, -32.5],
                        ]
                    ]
                ],
            }
        )

        _, written = self._prepare_all_children(api, tmp_path / "out", area)

        assert sorted(written["latitude"].unique()) == [-32.0, -31.0]
        assert len(written) == 2 * ROWS_PER_POLYGON
