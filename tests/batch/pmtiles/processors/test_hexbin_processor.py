import os
import shutil
import tempfile
import pandas
import pytest

from aodn_cloud_optimised.lib import DataQuery
from pathlib import Path
from unittest.mock import patch
from data_access_service import Config, API
from data_access_service.batch.pmtiles.processors.hexbin_processor import (
    HexbinProcessor,
)
from data_access_service.utils.pmtiles_utils import open_pmtiles
from tests.core.test_with_s3 import TestWithS3, REGION


class TestHexbinProcessor(TestWithS3):

    @pytest.fixture(scope="function")
    def upload_test_case_to_s3(self, aws_clients, setup_resources, mock_boto3_client):
        s3_client, _, _ = aws_clients
        TestWithS3.upload_to_s3(
            s3_client,
            DataQuery.BUCKET_OPTIMISED_DEFAULT,
            Path(__file__).parent.parent.parent.parent / "canned/s3_sample2",
        )

    @patch("aodn_cloud_optimised.lib.DataQuery.REGION", REGION)
    def test_hexbin_processor(
        self,
        aws_clients,
        upload_test_case_to_s3,
        mock_get_fs_token_paths,
        subset_request_factory,
        localstack,
    ):
        s3_client, _, _ = aws_clients
        config = Config.get_config()

        api = API()
        api.initialize_metadata()

        uuid = "541d4f15-122a-443d-ab4e-2b5feb08d6a0"
        dname = "animal_acoustic_tracking_delayed_qc.parquet"

        with patch("fsspec.core.get_fs_token_paths", mock_get_fs_token_paths):

            with tempfile.TemporaryDirectory() as tempdirname:

                try:
                    hex_processor = HexbinProcessor(
                        uuid=uuid, dataset_name=dname, work_dir=tempdirname, api=api
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

                    hex_processor.process()

                    staged_parquet_path = hex_processor.get_staged_path()

                    df = pandas.read_parquet(staged_parquet_path)
                    assert not df.empty, "df is empty"
                    assert list(df.columns) == [
                        "h_high",
                        "ym",
                        "c",
                    ], "df columns are not correct"
                    assert df[["h_high", "ym"]].duplicated().sum() == 0

                    geojsonseq_dir = hex_processor.get_geojsonseq_dir()

                    total_features = 0

                    for root, _, files in os.walk(geojsonseq_dir):
                        for file in files:
                            if file.endswith(".geojsonseq"):
                                path = os.path.join(root, file)

                                with open(path, "r", encoding="utf-8") as f:
                                    total_features += sum(1 for _ in f)

                    assert (
                        total_features == 12
                    ), f"total features should be 12 based on the test data, but got {total_features}"

                    # check pmtiles file exists
                    pmtiles_path = hex_processor.get_output_pmtiles_path()
                    assert os.path.exists(
                        pmtiles_path
                    ), f"pmtiles file not exists at {pmtiles_path}"

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
