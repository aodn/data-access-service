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
from data_access_service.models.pmtiles_types import (
    TIMELESS_DATE_PERIOD,
    TIMELESS_MONTH_PERIOD,
    TIMELESS_YEAR_PERIOD,
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
                            date_counts_by_cell[cell] = {
                                int(k[1:]): int(v)
                                for k, v in props.items()
                                if k.startswith("d") and k[1:].isdigit()
                            }

                    expected_by_cell = {}
                    for (h_high, d), c in expected_date.items():
                        expected_by_cell.setdefault(h_high, {})[d] = c

                    assert date_counts_by_cell == expected_by_cell, (
                        f"geojsonseq date properties mismatch.\n"
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
                    assert list(df.columns) == ["h_high", "y", "c"]
                    assert ((df["y"] >= 1000) & (df["y"] <= 9999)).all()

                    expected_year = _expected_staged_aggregates_from_input("year")
                    actual = {
                        (str(row.h_high), int(row.y)): int(row.c)
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
                            year_counts_by_cell[cell] = {
                                int(k[1:]): int(v)
                                for k, v in props.items()
                                if k.startswith("y") and k[1:].isdigit()
                            }

                    expected_by_cell = {}
                    for (h_high, y), c in expected_year.items():
                        expected_by_cell.setdefault(h_high, {})[y] = c

                    assert year_counts_by_cell == expected_by_cell, (
                        f"geojsonseq year properties mismatch.\n"
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
        """time_group_by=all → day staging with d/m/y properties on each feature."""
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

                    def _props_by_prefix(props, prefix):
                        return {
                            int(k[len(prefix) :]): int(v)
                            for k, v in props.items()
                            if k.startswith(prefix) and k[len(prefix) :].isdigit()
                        }

                    date_by_cell: Dict[str, Dict[int, int]] = {}
                    month_by_cell: Dict[str, Dict[int, int]] = {}
                    year_by_cell: Dict[str, Dict[int, int]] = {}
                    with gzip.open(high_res_path, "rt", encoding="utf-8") as f:
                        for line in f:
                            feature = json.loads(line)
                            props = feature["properties"]
                            cell = props["h"]
                            date_by_cell[cell] = _props_by_prefix(props, "d")
                            month_by_cell[cell] = _props_by_prefix(props, "m")
                            year_by_cell[cell] = _props_by_prefix(props, "y")
                            # Each feature must carry all three grains.
                            assert date_by_cell[cell]
                            assert month_by_cell[cell]
                            assert year_by_cell[cell]

                    def _by_cell(expected):
                        out: Dict[str, Dict[int, int]] = {}
                        for (h, p), c in expected.items():
                            out.setdefault(h, {})[p] = c
                        return out

                    assert date_by_cell == _by_cell(expected_date)
                    assert month_by_cell == _by_cell(expected_month)
                    assert year_by_cell == _by_cell(expected_year)

                    # Per-cell totals must match across grains.
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
        """No TIME column → single synthetic period; feature keys use grain prefix."""
        config = Config.get_config()
        api = API()
        api.initialize_metadata()

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):
            with tempfile.TemporaryDirectory() as tempdirname:
                try:
                    for grain, synthetic, prop_keys, period_col in (
                        (TimeGroupBy.MONTH, TIMELESS_MONTH_PERIOD, ["m197001"], "ym"),
                        (TimeGroupBy.DATE, TIMELESS_DATE_PERIOD, ["d19700101"], "d"),
                        (TimeGroupBy.YEAR, TIMELESS_YEAR_PERIOD, ["y1970"], "y"),
                        (
                            TimeGroupBy.ALL,
                            TIMELESS_DATE_PERIOD,
                            ["d19700101", "m197001", "y1970"],
                            "d",
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
                            for feature in features:
                                props = feature["properties"]
                                for prop_key in prop_keys:
                                    assert prop_key in props
                                    assert props[prop_key] > 0
                                period_props = sorted(
                                    k
                                    for k in props
                                    if k != "h" and len(k) > 1 and k[1:].isdigit()
                                )
                                assert period_props == sorted(prop_keys)

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


def test_period_property_key_prefixes():
    from data_access_service.batch.pmtiles.helpers.features_help import (
        apply_all_period_counts,
        apply_period_counts,
        period_property_key,
        rollup_day_counts,
    )
    from data_access_service.models.pmtiles_types import (
        TIMELESS_DATE_PERIOD,
        TIMELESS_MONTH_PERIOD,
        TIMELESS_YEAR_PERIOD,
    )

    assert period_property_key(20240115, TimeGroupBy.DATE) == "d20240115"
    assert period_property_key(202401, TimeGroupBy.MONTH) == "m202401"
    assert period_property_key(2022, TimeGroupBy.YEAR) == "y2022"
    assert period_property_key(TIMELESS_DATE_PERIOD, TimeGroupBy.DATE) == "d19700101"
    assert period_property_key(TIMELESS_MONTH_PERIOD, TimeGroupBy.MONTH) == "m197001"
    assert period_property_key(TIMELESS_YEAR_PERIOD, TimeGroupBy.YEAR) == "y1970"

    props: dict = {"h": "abc"}
    apply_period_counts(props, {20240115: 3, 20240116: 0}, TimeGroupBy.DATE)
    assert props == {"h": "abc", "d20240115": 3}

    year_props: dict = {"h": "abc"}
    apply_period_counts(year_props, {2022: 5, 2023: 0}, TimeGroupBy.YEAR)
    assert year_props == {"h": "abc", "y2022": 5}

    rolled = rollup_day_counts({20240115: 3, 20240116: 2, 20240201: 1, 20250101: 4})
    assert rolled[TimeGroupBy.DATE] == {
        20240115: 3,
        20240116: 2,
        20240201: 1,
        20250101: 4,
    }
    assert rolled[TimeGroupBy.MONTH] == {202401: 5, 202402: 1, 202501: 4}
    assert rolled[TimeGroupBy.YEAR] == {2024: 6, 2025: 4}

    all_props: dict = {"h": "abc"}
    apply_all_period_counts(all_props, {20240115: 3, 20240116: 2})
    assert all_props == {
        "h": "abc",
        "d20240115": 3,
        "d20240116": 2,
        "m202401": 5,
        "y2024": 5,
    }


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
