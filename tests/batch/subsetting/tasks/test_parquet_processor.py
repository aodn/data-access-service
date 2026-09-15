import pytz
import pandas as pd
import pytest

from shapely.geometry import box
from unittest.mock import patch, MagicMock
from data_access_service.batch.subsetting.tasks.parquet_processor import (
    trim_date_range,
    query_data,
    _filter_partition_by_polygon,
)


class TestGenerateFunctions:

    @pytest.fixture
    def mock_api(self):
        return MagicMock()

    @pytest.fixture
    def mock_aws(self):
        return MagicMock()

    def test_trim_date_range_within_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            pd.Timestamp(year=2020, month=1, day=1, tz=pytz.UTC),
            pd.Timestamp(year=2022, month=12, day=31, tz=pytz.UTC),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            requested_start_date=pd.Timestamp(year=2021, month=1, day=1),
            requested_end_date=pd.Timestamp(year=2021, month=12, day=31),
        )
        assert start_date == pd.Timestamp(year=2021, month=1, day=1)
        assert end_date == pd.Timestamp(year=2021, month=12, day=31)

    def test_trim_date_range_out_of_bounds(self, mock_api):
        mock_api.get_temporal_extent.return_value = [
            pd.Timestamp(year=2020, month=1, day=1, tz=pytz.UTC),
            pd.Timestamp(year=2022, month=12, day=31, tz=pytz.UTC),
        ]
        start_date, end_date = trim_date_range(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            requested_start_date=pd.Timestamp(year=2019, month=1, day=1),
            requested_end_date=pd.Timestamp(year=2023, month=1, day=1),
        )
        assert start_date == pd.Timestamp(year=2020, month=1, day=1)
        assert end_date == pd.Timestamp(year=2022, month=12, day=31)

    @patch("data_access_service.batch.subsetting.tasks.parquet_processor.API")
    def test_query_data_no_data(self, mock_api):
        mock_api.get_dataset.return_value = None
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            start_date=pd.Timestamp(year=2021, month=1, day=1),
            end_date=pd.Timestamp(year=2021, month=12, day=31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is None

    @patch("data_access_service.batch.subsetting.tasks.parquet_processor.API")
    def test_query_data_with_data(self, mock_api):
        mock_api.get_dataset_data.return_value = MagicMock(empty=False)
        result = query_data(
            api=mock_api,
            uuid="test-uuid",
            key="test-key",
            start_date=pd.Timestamp(year=2021, month=1, day=1),
            end_date=pd.Timestamp(year=2021, month=12, day=31),
            min_lat=-10,
            max_lat=10,
            min_lon=-20,
            max_lon=20,
        )
        assert result is not None

    def test_filter_partition_by_polygon_keeps_points_on_edge(self):
        df = pd.DataFrame(
            {
                "site": ["antimeridian", "north_pole", "inside"],
                "LATITUDE": [-16.77, 90.0, 10.0],
                "LONGITUDE": [-180.0, 0.0, 20.0],
            }
        )
        result = _filter_partition_by_polygon(
            df,
            shapely_poly=box(-180, -90, 180, 90),
            lat_key="LATITUDE",
            lon_key="LONGITUDE",
        )
        assert list(result["site"]) == ["antimeridian", "north_pole", "inside"]
        assert "geometry" not in result.columns

    def test_filter_partition_by_polygon_drops_points_outside(self):
        df = pd.DataFrame(
            {
                "site": ["inside", "edge", "outside"],
                "LATITUDE": [5.0, 10.0, 10.001],
                "LONGITUDE": [5.0, 10.0, 5.0],
            }
        )
        result = _filter_partition_by_polygon(
            df,
            shapely_poly=box(0, 0, 10, 10),
            lat_key="LATITUDE",
            lon_key="LONGITUDE",
        )
        assert list(result["site"]) == ["inside", "edge"]
