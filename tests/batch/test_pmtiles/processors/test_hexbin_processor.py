import json
import os
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Dict, Tuple
from unittest.mock import patch

import gzip
import h3
import pandas
import pytest
from aodn_cloud_optimised.lib import DataQuery

from data_access_service import Config, API
from data_access_service.batch.pmtiles.processors.hexbin_processor import (
    HexbinProcessor,
)
from data_access_service.core.duckdbclient import PmTileDuckDBClient
from data_access_service.batch.pmtiles.helpers.features_help import (
    build_counts_tree_from_days,
    build_counts_tree_from_months,
    build_counts_tree_from_years,
)
from data_access_service.models.pmtiles_types import (
    COUNTS_PROPERTY,
    DAYS_KEY,
    TIMELESS_DATE_PERIOD,
    TIMELESS_MONTH_PERIOD,
    TIMELESS_YEAR_PERIOD,
    TOTAL_KEY,
    TimeGroupBy,
)
from data_access_service.utils.pmtiles_utils import open_pmtiles
from tests.core.test_with_s3 import TestWithS3, REGION

ANIMAL_ACOUSTIC_UUID = "541d4f15-122a-443d-ab4e-2b5feb08d6a0"
ANIMAL_ACOUSTIC_DNAME = "animal_acoustic_tracking_delayed_qc.parquet"
ANIMAL_ACOUSTIC_CANNED_DIR = (
    Path(__file__).parent.parent.parent.parent
    / "canned"
    / "s3_sample2"
    / ANIMAL_ACOUSTIC_DNAME
)
# Max H3 resolution used by hex layers in config.yaml (hex_z10).
HEX_MAX_RES = 8
# Mapped TIME column for animal_acoustic_tracking_delayed_qc.
TIME_COL = "detection_timestamp"
LON_COL = "longitude"
LAT_COL = "latitude"


def _expected_staged_aggregates_from_input(
    period: str,
) -> Dict[Tuple[str, int], int]:
    """
    Independently aggregate the canned input parquet the same way staging does:
    H3 cell at max res + year (YYYY), year-month (YYYYMM), or date (YYYYMMDD) key.
    """
    fmt_by_period = {"year": "%Y", "month": "%Y%m", "date": "%Y%m%d"}
    if period not in fmt_by_period:
        raise ValueError(
            f"period must be one of {sorted(fmt_by_period)}, got {period!r}"
        )

    df = pandas.read_parquet(ANIMAL_ACOUSTIC_CANNED_DIR)
    ts = pandas.to_datetime(df[TIME_COL], utc=False)
    period_key = ts.dt.strftime(fmt_by_period[period]).astype(int)
    cells = [
        h3.latlng_to_cell(float(lat), float(lon), HEX_MAX_RES)
        for lat, lon in zip(df[LAT_COL], df[LON_COL])
    ]
    grouped = (
        pandas.DataFrame({"h_high": cells, "period": period_key})
        .groupby(["h_high", "period"], sort=False)
        .size()
    )
    return {(str(h), int(p)): int(c) for (h, p), c in grouped.items()}


def _parse_counts_tree(props: dict) -> dict:
    """Parse feature property ``c`` (JSON string) to nested map."""
    raw = props[COUNTS_PROPERTY]
    assert isinstance(raw, str), "counts property must be a JSON string for MVT"
    return json.loads(raw)


def _flatten_days_from_tree(tree: dict) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for y, year_node in tree.items():
        for m, month_node in year_node.items():
            if m == TOTAL_KEY:
                continue
            for d, count in month_node.get(DAYS_KEY, {}).items():
                out[int(f"{y}{m}{d}")] = int(count)
    return out


def _flatten_months_from_tree(tree: dict) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for y, year_node in tree.items():
        for m, month_node in year_node.items():
            if m == TOTAL_KEY:
                continue
            out[int(f"{y}{m}")] = int(month_node[TOTAL_KEY])
    return out


def _flatten_years_from_tree(tree: dict) -> Dict[int, int]:
    return {int(y): int(year_node[TOTAL_KEY]) for y, year_node in tree.items()}


def _by_cell_period(expected: Dict[Tuple[str, int], int]) -> Dict[str, Dict[int, int]]:
    out: Dict[str, Dict[int, int]] = {}
    for (h, p), c in expected.items():
        out.setdefault(h, {})[p] = c
    return out


class TestHexbinProcessor(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent.parent.parent / "canned/s3_sample2",
        )

    def _configure_hex_processor(
        self, tempdirname, api, localstack, time_group_by: TimeGroupBy | None = None
    ):
        hex_processor = HexbinProcessor(
            uuid=ANIMAL_ACOUSTIC_UUID,
            dataset_name=ANIMAL_ACOUSTIC_DNAME,
            work_dir=tempdirname,
            api=api,
        )
        if time_group_by is not None:
            hex_processor.pmtiles_config = replace(
                hex_processor.pmtiles_config, time_group_by=time_group_by
            )
        hex_processor.pm_client.execute(
            f"""
                    SET s3_endpoint='{localstack.get_url().replace("http://", "")}';
                    SET s3_region='{REGION}';
                    SET s3_access_key_id='test';
                    SET s3_secret_access_key='test';
                    SET s3_url_style='path';
                    SET s3_use_ssl=false;
                """
        )
        return hex_processor

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_hexbin_processor_by_month(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
        localstack,
    ):
        config = Config.get_config()

        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):

            with tempfile.TemporaryDirectory() as tempdirname:

                try:
                    hex_processor = self._configure_hex_processor(
                        tempdirname, api, localstack
                    )
                    assert (
                        hex_processor.pmtiles_config.time_group_by == TimeGroupBy.MONTH
                    )

                    # Run the pipeline steps individually, in the same order as
                    # process(), so intermediate outputs can be asserted before
                    # the cleanup steps remove them.
                    hex_processor.build_staging_parquet()

                    staged_parquet_path = hex_processor.get_staged_path()

                    df = pandas.read_parquet(staged_parquet_path)
                    assert not df.empty, "df is empty"
                    assert list(df.columns) == [
                        "h_high",
                        "ym",
                        "c",
                    ], "df columns are not correct"
                    assert df[["h_high", "ym"]].duplicated().sum() == 0
                    assert (
                        (df["ym"] >= 100001) & (df["ym"] <= 999912)
                    ).all(), "ym values should be YYYYMM integers"

                    # Staging (h_high, ym, c) must match independent aggregation
                    # of the canned input parquet (detection_timestamp -> YYYYMM).
                    expected_month = _expected_staged_aggregates_from_input("month")
                    actual_month = {
                        (str(row.h_high), int(row.ym)): int(row.c)
                        for row in df.itertuples(index=False)
                    }
                    assert actual_month == expected_month, (
                        f"staged (h_high, ym) counts do not match input parquet.\n"
                        f"expected={expected_month}\n"
                        f"actual={actual_month}"
                    )
                    assert sum(actual_month.values()) == len(
                        pandas.read_parquet(ANIMAL_ACOUSTIC_CANNED_DIR)
                    )

                    geojsonseq_paths = hex_processor.generate_geojsonseq_files()

                    geojsonseq_dir = hex_processor.get_geojsonseq_dir()

                    total_features = 0

                    for root, _, files in os.walk(geojsonseq_dir):
                        for file in files:
                            if file.endswith(".geojsonseq.gz"):
                                path = os.path.join(root, file)

                                with gzip.open(path, "r") as f:
                                    total_features += sum(1 for _ in f)

                    assert (
                        total_features == 12
                    ), f"total features should be 12 based on the test data, but got {total_features}"

                    metadata_path = hex_processor.generate_metadata_json()
                    assert os.path.exists(metadata_path)
                    assert metadata_path == hex_processor.get_metadata_path()
                    # Same folder as the pmtiles output, named {dname}.metadata
                    assert os.path.dirname(metadata_path) == os.path.dirname(
                        hex_processor.get_output_pmtiles_path()
                    )
                    assert (
                        os.path.basename(metadata_path)
                        == f"{ANIMAL_ACOUSTIC_DNAME}.metadata"
                    )
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)
                    period_keys = [p for (_, p) in expected_month.keys()]
                    assert metadata["min_date"] == min(period_keys)
                    assert metadata["max_date"] == max(period_keys)
                    assert metadata["time_group_by"] == "month"
                    assert metadata["has_time"] is True
                    assert "last_updated" in metadata
                    assert metadata["last_updated"]  # non-empty ISO timestamp

                    hex_processor._remove_staged_parquet()
                    assert not os.path.exists(
                        staged_parquet_path
                    ), "staged parquet should be removed before tippecanoe"

                    # Production order: release DuckDB *before* tippecanoe so the
                    # buffer pool is freed. A prior duckdb.close() double-teardown
                    # SIGSEGV'd forked workers here (wait status 139) after real
                    # httpfs/h3 work — so this step must run in integration tests,
                    # not only unit mocks of shutdown.
                    assert PmTileDuckDBClient._global_db_connection is not None
                    hex_processor._release_duckdb("before tippecanoe")
                    assert PmTileDuckDBClient._global_db_connection is None
                    assert PmTileDuckDBClient._temp_dir_object is None
                    # Idempotent: process() finally also calls release.
                    hex_processor._release_duckdb("after process cleanup")

                    hex_processor.generate_pmtiles_file(
                        geojsonseq_paths=geojsonseq_paths
                    )

                    # check pmtiles file exists
                    pmtiles_path = hex_processor.get_output_pmtiles_path()
                    assert os.path.exists(
                        pmtiles_path
                    ), f"pmtiles file not exists at {pmtiles_path}"

                    hex_processor._remove_geojsonseq_files(geojsonseq_paths)
                    for path in geojsonseq_paths:
                        assert not os.path.exists(
                            path
                        ), "geojsonseq files should be removed after pmtiles generation"

                    with open_pmtiles(pmtiles_path) as reader:
                        header = reader.header()
                        metadata = reader.metadata()

                        hex_processor.logger.info("metadata:")
                        hex_processor.logger.info(metadata)

                        assert header["min_zoom"] == 0, "min zoom should be 0"
                        assert header["max_zoom"] == 12, "max zoom should be 12"
                        assert (
                            "vector_layers" in metadata
                        ), "metadata should contain vector_layers"
                        assert (
                            len(metadata["vector_layers"]) > 0
                        ), "vector_layers should not be empty"

                except Exception as ex:
                    # Should not land here
                    assert False, f"{ex}"
                finally:
                    # Delete temp output folder as the name always same for testing
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_animal_acoustic_aggregates_by_date(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        localstack,
    ):
        """
        Verify day-level (YYYYMMDD) count aggregation when time_group_by=date
        for animal_acoustic_tracking_delayed_qc.parquet from s3_sample2.
        """
        config = Config.get_config()
        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with tempfile.TemporaryDirectory() as tempdirname:
                try:
                    hex_processor = self._configure_hex_processor(
                        tempdirname,
                        api,
                        localstack,
                        time_group_by=TimeGroupBy.DATE,
                    )
                    hex_processor.build_staging_parquet()

                    df = pandas.read_parquet(hex_processor.get_staged_path())
                    assert list(df.columns) == ["h_high", "d", "c"]
                    assert ((df["d"] >= 10000101) & (df["d"] <= 99991231)).all()

                    # Staging (h_high, d, c) must match independent aggregation
                    # of the canned input parquet (detection_timestamp -> YYYYMMDD).
                    expected_date = _expected_staged_aggregates_from_input("date")
                    actual = {
                        (str(row.h_high), int(row.d)): int(row.c)
                        for row in df.itertuples(index=False)
                    }
                    assert actual == expected_date, (
                        f"staged (h_high, d) counts do not match input parquet.\n"
                        f"expected={expected_date}\n"
                        f"actual={actual}"
                    )
                    assert sum(actual.values()) == len(
                        pandas.read_parquet(ANIMAL_ACOUSTIC_CANNED_DIR)
                    )

                    geojsonseq_paths = hex_processor.generate_geojsonseq_files()
                    high_res_path = next(
                        p
                        for p in geojsonseq_paths
                        if p.endswith("hex_z10.geojsonseq.gz")
                    )
                    date_counts_by_cell = {}
                    with gzip.open(high_res_path, "rt", encoding="utf-8") as f:
                        for line in f:
                            feature = json.loads(line)
                            props = feature["properties"]
                            cell = props["h"]
                            tree = _parse_counts_tree(props)
                            days = _flatten_days_from_tree(tree)
                            date_counts_by_cell[cell] = days
                            # Month/year totals must match day rollups.
                            month_from_days: Dict[int, int] = {}
                            year_from_days: Dict[int, int] = {}
                            for d, c in days.items():
                                month_from_days[d // 100] = (
                                    month_from_days.get(d // 100, 0) + c
                                )
                                year_from_days[d // 10000] = (
                                    year_from_days.get(d // 10000, 0) + c
                                )
                            assert _flatten_months_from_tree(tree) == month_from_days
                            assert _flatten_years_from_tree(tree) == year_from_days

                    expected_by_cell = _by_cell_period(expected_date)
                    assert date_counts_by_cell == expected_by_cell, (
                        f"geojsonseq date tree mismatch.\n"
                        f"expected={expected_by_cell}\n"
                        f"actual={date_counts_by_cell}"
                    )

                    metadata_path = hex_processor.generate_metadata_json()
                    assert os.path.exists(metadata_path)
                    assert (
                        os.path.basename(metadata_path)
                        == f"{ANIMAL_ACOUSTIC_DNAME}.metadata"
                    )
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)
                    period_keys = [p for (_, p) in expected_date.keys()]
                    assert metadata["min_date"] == min(period_keys)
                    assert metadata["max_date"] == max(period_keys)
                    assert metadata["time_group_by"] == "date"
                    assert metadata["has_time"] is True
                    assert "last_updated" in metadata
                    assert metadata["last_updated"]
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_animal_acoustic_aggregates_by_year(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        localstack,
    ):
        """
        Verify year-level (YYYY) count aggregation when time_group_by=year
        for animal_acoustic_tracking_delayed_qc.parquet from s3_sample2.
        """
        config = Config.get_config()
        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with tempfile.TemporaryDirectory() as tempdirname:
                try:
                    hex_processor = self._configure_hex_processor(
                        tempdirname,
                        api,
                        localstack,
                        time_group_by=TimeGroupBy.YEAR,
                    )
                    hex_processor.build_staging_parquet()

                    df = pandas.read_parquet(hex_processor.get_staged_path())
                    assert list(df.columns) == ["h_high", "yr", "c"]
                    assert ((df["yr"] >= 1000) & (df["yr"] <= 9999)).all()

                    expected_year = _expected_staged_aggregates_from_input("year")
                    actual = {
                        (str(row.h_high), int(row.yr)): int(row.c)
                        for row in df.itertuples(index=False)
                    }
                    assert actual == expected_year, (
                        f"staged (h_high, y) counts do not match input parquet.\n"
                        f"expected={expected_year}\n"
                        f"actual={actual}"
                    )
                    assert sum(actual.values()) == len(
                        pandas.read_parquet(ANIMAL_ACOUSTIC_CANNED_DIR)
                    )

                    geojsonseq_paths = hex_processor.generate_geojsonseq_files()
                    high_res_path = next(
                        p
                        for p in geojsonseq_paths
                        if p.endswith("hex_z10.geojsonseq.gz")
                    )
                    year_counts_by_cell = {}
                    with gzip.open(high_res_path, "rt", encoding="utf-8") as f:
                        for line in f:
                            feature = json.loads(line)
                            props = feature["properties"]
                            cell = props["h"]
                            tree = _parse_counts_tree(props)
                            # Year-only tree has no month children.
                            for year_node in tree.values():
                                assert set(year_node.keys()) == {TOTAL_KEY}
                            year_counts_by_cell[cell] = _flatten_years_from_tree(tree)

                    expected_by_cell = _by_cell_period(expected_year)
                    assert year_counts_by_cell == expected_by_cell, (
                        f"geojsonseq year tree mismatch.\n"
                        f"expected={expected_by_cell}\n"
                        f"actual={year_counts_by_cell}"
                    )

                    metadata_path = hex_processor.generate_metadata_json()
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)
                    period_keys = [p for (_, p) in expected_year.keys()]
                    assert metadata["min_date"] == min(period_keys)
                    assert metadata["max_date"] == max(period_keys)
                    assert metadata["time_group_by"] == "year"
                    assert metadata["has_time"] is True
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_animal_acoustic_aggregates_by_all(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        localstack,
    ):
        """time_group_by=all → day staging; nested c tree with day/month/year totals."""
        config = Config.get_config()
        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with tempfile.TemporaryDirectory() as tempdirname:
                try:
                    hex_processor = self._configure_hex_processor(
                        tempdirname,
                        api,
                        localstack,
                        time_group_by=TimeGroupBy.ALL,
                    )
                    hex_processor.build_staging_parquet()

                    df = pandas.read_parquet(hex_processor.get_staged_path())
                    assert list(df.columns) == ["h_high", "d", "c"]

                    expected_date = _expected_staged_aggregates_from_input("date")
                    expected_month = _expected_staged_aggregates_from_input("month")
                    expected_year = _expected_staged_aggregates_from_input("year")
                    actual = {
                        (str(row.h_high), int(row.d)): int(row.c)
                        for row in df.itertuples(index=False)
                    }
                    assert actual == expected_date

                    geojsonseq_paths = hex_processor.generate_geojsonseq_files()
                    high_res_path = next(
                        p
                        for p in geojsonseq_paths
                        if p.endswith("hex_z10.geojsonseq.gz")
                    )

                    date_by_cell: Dict[str, Dict[int, int]] = {}
                    month_by_cell: Dict[str, Dict[int, int]] = {}
                    year_by_cell: Dict[str, Dict[int, int]] = {}
                    with gzip.open(high_res_path, "rt", encoding="utf-8") as f:
                        for line in f:
                            feature = json.loads(line)
                            props = feature["properties"]
                            cell = props["h"]
                            tree = _parse_counts_tree(props)
                            date_by_cell[cell] = _flatten_days_from_tree(tree)
                            month_by_cell[cell] = _flatten_months_from_tree(tree)
                            year_by_cell[cell] = _flatten_years_from_tree(tree)
                            assert date_by_cell[cell]
                            assert month_by_cell[cell]
                            assert year_by_cell[cell]

                    assert date_by_cell == _by_cell_period(expected_date)
                    assert month_by_cell == _by_cell_period(expected_month)
                    assert year_by_cell == _by_cell_period(expected_year)

                    for cell in date_by_cell:
                        day_total = sum(date_by_cell[cell].values())
                        assert day_total == sum(month_by_cell[cell].values())
                        assert day_total == sum(year_by_cell[cell].values())

                    metadata_path = hex_processor.generate_metadata_json()
                    with open(metadata_path, encoding="utf-8") as f:
                        metadata = json.load(f)
                    period_keys = [p for (_, p) in expected_date.keys()]
                    assert metadata["min_date"] == min(period_keys)
                    assert metadata["max_date"] == max(period_keys)
                    assert metadata["time_group_by"] == "all"
                    assert metadata["has_time"] is True
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_timeless_dataset_uses_synthetic_period(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        localstack,
    ):
        """No TIME column → synthetic period; nested c tree for the grain."""
        config = Config.get_config()
        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with tempfile.TemporaryDirectory() as tempdirname:
                try:
                    for grain, synthetic, period_col, check_tree in (
                        (
                            TimeGroupBy.MONTH,
                            TIMELESS_MONTH_PERIOD,
                            "ym",
                            lambda tree, n: (
                                tree == {"1970": {TOTAL_KEY: n, "01": {TOTAL_KEY: n}}}
                            ),
                        ),
                        (
                            TimeGroupBy.DATE,
                            TIMELESS_DATE_PERIOD,
                            "d",
                            lambda tree, n: (
                                tree
                                == {
                                    "1970": {
                                        TOTAL_KEY: n,
                                        "01": {
                                            TOTAL_KEY: n,
                                            DAYS_KEY: {"01": n},
                                        },
                                    }
                                }
                            ),
                        ),
                        (
                            TimeGroupBy.YEAR,
                            TIMELESS_YEAR_PERIOD,
                            "yr",
                            lambda tree, n: (tree == {"1970": {TOTAL_KEY: n}}),
                        ),
                        (
                            TimeGroupBy.ALL,
                            TIMELESS_DATE_PERIOD,
                            "d",
                            lambda tree, n: (
                                tree
                                == {
                                    "1970": {
                                        TOTAL_KEY: n,
                                        "01": {
                                            TOTAL_KEY: n,
                                            DAYS_KEY: {"01": n},
                                        },
                                    }
                                }
                            ),
                        ),
                    ):
                        hex_processor = self._configure_hex_processor(
                            tempdirname,
                            api,
                            localstack,
                            time_group_by=grain,
                        )
                        with patch.object(
                            hex_processor, "get_time_col_name", return_value=None
                        ):
                            hex_processor.build_staging_parquet()
                            staged = pandas.read_parquet(
                                hex_processor.get_staged_path()
                            )
                            assert list(staged.columns) == [
                                "h_high",
                                period_col,
                                "c",
                            ]
                            assert (staged[period_col] == synthetic).all()
                            total_c = int(staged["c"].sum())
                            assert total_c == len(
                                pandas.read_parquet(ANIMAL_ACOUSTIC_CANNED_DIR)
                            )

                            geojsonseq_paths = hex_processor.generate_geojsonseq_files()
                            high_res_path = next(
                                p
                                for p in geojsonseq_paths
                                if p.endswith("hex_z10.geojsonseq.gz")
                            )
                            with gzip.open(high_res_path, "rt", encoding="utf-8") as f:
                                features = [json.loads(line) for line in f]
                            assert features
                            feature_total = 0
                            for feature in features:
                                props = feature["properties"]
                                assert set(props.keys()) >= {"h", COUNTS_PROPERTY}
                                tree = _parse_counts_tree(props)
                                n = _flatten_years_from_tree(tree)
                                assert len(n) == 1 and 1970 in n
                                feature_total += n[1970]
                                # Per-feature tree shape: only synthetic buckets.
                                cell_count = n[1970]
                                assert check_tree(tree, cell_count)
                            assert feature_total == total_c

                            metadata_path = hex_processor.generate_metadata_json()
                            with open(metadata_path, encoding="utf-8") as f:
                                metadata = json.load(f)
                            assert metadata["min_date"] == synthetic
                            assert metadata["max_date"] == synthetic
                            assert metadata["time_group_by"] == grain.value
                            assert metadata["has_time"] is False

                            # Clean intermediates between grain runs
                            hex_processor._remove_staged_parquet()
                            hex_processor._remove_geojsonseq_files(geojsonseq_paths)
                finally:
                    shutil.rmtree(config.get_temp_folder("888"), ignore_errors=True)


def test_time_group_by_default_and_invalid():
    config = Config.get_config()
    pm = config.get_pmtiles_config()
    assert pm.time_group_by == TimeGroupBy.MONTH

    original = config.config["pmtiles"]["config"].get("time_group_by")
    try:
        config.config["pmtiles"]["config"]["time_group_by"] = "week"
        with pytest.raises(ValueError, match="time_group_by"):
            config.get_pmtiles_config()
    finally:
        if original is None:
            config.config["pmtiles"]["config"].pop("time_group_by", None)
        else:
            config.config["pmtiles"]["config"]["time_group_by"] = original


def test_nested_counts_tree_encoding():
    from data_access_service.batch.pmtiles.helpers.features_help import (
        apply_counts_tree,
        build_hex_feature,
        counts_tree_for_grain,
    )

    day_tree = build_counts_tree_from_days(
        {20240115: 3, 20240116: 2, 20240201: 1, 20250101: 4}
    )
    assert day_tree == {
        "2024": {
            TOTAL_KEY: 6,
            "01": {TOTAL_KEY: 5, DAYS_KEY: {"15": 3, "16": 2}},
            "02": {TOTAL_KEY: 1, DAYS_KEY: {"01": 1}},
        },
        "2025": {
            TOTAL_KEY: 4,
            "01": {TOTAL_KEY: 4, DAYS_KEY: {"01": 4}},
        },
    }

    month_tree = build_counts_tree_from_months({202401: 5, 202402: 1, 202501: 4})
    assert month_tree == {
        "2024": {TOTAL_KEY: 6, "01": {TOTAL_KEY: 5}, "02": {TOTAL_KEY: 1}},
        "2025": {TOTAL_KEY: 4, "01": {TOTAL_KEY: 4}},
    }

    year_tree = build_counts_tree_from_years({2022: 5, 2023: 0, 2024: 6})
    assert year_tree == {
        "2022": {TOTAL_KEY: 5},
        "2024": {TOTAL_KEY: 6},
    }

    props: dict = {"h": "abc"}
    apply_counts_tree(props, day_tree)
    assert props["h"] == "abc"
    assert json.loads(props[COUNTS_PROPERTY]) == day_tree

    cell = h3.latlng_to_cell(-42.0, 147.0, 8)
    feature = build_hex_feature(
        cell=cell,
        period_counts={20240115: 3, 20240116: 2},
        layer_name="hex_z10",
        minzoom=0,
        maxzoom=10,
        include_tippecanoe_metadata=False,
        grain=TimeGroupBy.ALL,
    )
    assert set(feature["properties"].keys()) == {"h", COUNTS_PROPERTY}
    assert feature["properties"]["h"] == cell
    parsed = json.loads(feature["properties"][COUNTS_PROPERTY])
    assert parsed == counts_tree_for_grain({20240115: 3, 20240116: 2}, TimeGroupBy.ALL)
    assert parsed["2024"][TOTAL_KEY] == 5
    assert parsed["2024"]["01"][TOTAL_KEY] == 5


def test_sidecar_metadata_has_time_roundtrip():
    from data_access_service.models.pmtiles_types import PmtilesSidecarMetadata

    timed = PmtilesSidecarMetadata(
        min_date=20240101,
        max_date=20240101,
        time_group_by=TimeGroupBy.DATE,
        last_updated="2026-01-01T00:00:00+00:00",
        has_time=True,
    )
    assert timed.to_dict()["has_time"] is True
    assert PmtilesSidecarMetadata.from_dict(timed.to_dict()).has_time is True

    timeless = PmtilesSidecarMetadata(
        min_date=TIMELESS_DATE_PERIOD,
        max_date=TIMELESS_DATE_PERIOD,
        time_group_by=TimeGroupBy.DATE,
        last_updated="2026-01-01T00:00:00+00:00",
        has_time=False,
    )
    assert timeless.to_dict()["has_time"] is False
    assert PmtilesSidecarMetadata.from_dict(timeless.to_dict()).has_time is False

    # Legacy sidecar without has_time defaults to True (real time)
    legacy = {
        "min_date": 19700101,
        "max_date": 19700101,
        "time_group_by": "date",
        "last_updated": "2026-01-01T00:00:00+00:00",
    }
    assert PmtilesSidecarMetadata.from_dict(legacy).has_time is True


def test_build_time_key_expressions():
    from data_access_service.core.duckdbclient import PmTileDuckDBClient

    ym = PmTileDuckDBClient.build_ym_expression("TIMESTAMP", "timestamp")
    assert "%Y%m" in ym
    assert "%Y%m%d" not in ym

    d = PmTileDuckDBClient.build_date_key_expression("TIMESTAMP", "timestamp")
    assert "%Y%m%d" in d

    y = PmTileDuckDBClient.build_year_key_expression("TIMESTAMP", "timestamp")
    assert "%Y" in y
    assert "%Y%m" not in y
    assert "%Y%m%d" not in y
