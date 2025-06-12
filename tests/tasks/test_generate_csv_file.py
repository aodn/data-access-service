from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

from data_access_service.batch.tasks.generate_csv_file import (
    trim_date_range,
    query_data,
    generate_zip_name,
)


class TestGenerateCSVFile:

    @pytest.fixture
    def mock_api(self):
        return MagicMock()

    @pytest.fixture
    def mock_aws(self):
        return MagicMock()

    def test_trim_date_range_within_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            datetime(2020, 1, 1),
            datetime(2022, 12, 31),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            requested_start_date=datetime(2021, 1, 1),
            requested_end_date=datetime(2021, 12, 31),
        )
        assert start_date == datetime(2021, 1, 1)
        assert end_date == datetime(2021, 12, 31)

    def test_trim_date_range_out_of_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            datetime(2020, 1, 1),
            datetime(2022, 12, 31),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            requested_start_date=datetime(2019, 1, 1),
            requested_end_date=datetime(2023, 1, 1),
        )
        assert start_date == datetime(2020, 1, 1)
        assert end_date == datetime(2022, 12, 31)

    @patch("data_access_service.tasks.generate_csv_file.API")
    def test_query_data_no_data(self, mock_api):
        mock_api.get_dataset_data.return_value = None
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            start_date=datetime(2021, 1, 1),
            end_date=datetime(2021, 12, 31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is None

    @patch("data_access_service.tasks.generate_csv_file.API")
    def test_query_data_with_data(self, mock_api):
        mock_api.get_dataset_data.return_value = MagicMock(empty=False)
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            start_date=datetime(2021, 1, 1),
            end_date=datetime(2021, 12, 31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is not None

    def test_generate_zip_name(self):
        uuid = "test-uuid"
        start_date = datetime(2021, 1, 1)
        end_date = datetime(2021, 12, 31)
        zip_name = generate_zip_name(uuid, start_date, end_date)
        assert zip_name == "test-uuid_2021-01-01_2021-12-31"
