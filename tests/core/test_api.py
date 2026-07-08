import json
import unittest
from typing import Dict, Any

import dask.dataframe as ddf
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock

from aodn_cloud_optimised.lib import DataQuery

from data_access_service import API
from data_access_service.core.api import BaseAPI
from data_access_service.utils.routes_helper import (
    _generate_partial_json_array,
    _response_json,
)


class TestApi(unittest.TestCase):
    # set a middle check point to check the procedure in the API init function
    def setUp(self):
        self.middle_check = None

    # Use this canned data as the metadata map
    with open(
        Path(__file__).resolve().parent.parent / "canned/catalog_uncached.json", "r"
    ) as file:

        @patch.object(
            DataQuery.Metadata,
            "metadata_catalog_uncached",
            return_value=json.load(file),
        )
        def test_map_column_names(self, get_metadata):
            api = API()
            api.initialize_metadata()

            uuid = "541d4f15-122a-443d-ab4e-2b5feb08d6a0"
            key = "animal_acoustic_tracking_delayed_qc.parquet"

            md: Dict[str, Any] = api.get_raw_meta_data(uuid)
            d = md.get(key)
            meta: dict = d.get("dataset_metadata")
            self.assertEqual(
                meta.get("title").casefold(),
                "AATAMS ACOUSTIC".casefold(),
                "Title equals",
            )

            # Now if you try to map the field, since this metadata do not have TIME, so it should return timestamp
            # this record do not have DEPTH field, so we will remove it from query after map, the field is of small
            # letter for this recordset
            col = api.map_column_names(
                uuid,
                key,
                ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(
                col, ["detection_timestamp", "latitude", "longitude"], "TIME mapped"
            )

            # the lon_varname and lat_varname should map correctly, they are expected to be lowercases
            col = api.map_column_names(uuid, key, ["LATITUDE"])
            lat_varname = col[0]
            self.assertEqual(lat_varname, "latitude")

            col = api.map_column_names(uuid, key, ["LONGITUDE"])
            lon_varname = col[0]
            self.assertEqual(lon_varname, "longitude")

            # This uuid have time so it will not map
            col = api.map_column_names(
                "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
                "autonomous_underwater_vehicle.parquet",
                ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(
                col,
                ["TIME", "LATITUDE", "LONGITUDE"],
                "TIME no need to map",
            )

            # This uuid have JULD but no time and timestamp, so map it to JULD
            col = api.map_column_names(
                "95d6314c-cfc7-40ae-b439-85f14541db71",
                "animal_ctd_satellite_relay_tagging_delayed_qc.parquet",
                ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(col, ["JULD", "LATITUDE", "LONGITUDE"], "TIME mapped")

            # This uuid uses eventDate as temporal field
            col = api.map_column_names(
                "ec2c0ef9-3645-4ded-b617-c8297f6eb250",
                "aggregated_seabird_nonqc.parquet",
                ["TIME", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(
                col,
                ["eventDate", "decimalLatitude", "decimalLongitude"],
                "TIME mapped to eventDate, LATITUDE mapped to decimalLatitude, LONGITUDE mapped to decimalLongitude",
            )

    def test_nan_to_none_conversion(self):
        # Create a sample pandas DataFrame with NaN values
        data = {
            "TIME": ["2023-01-01", "2023-01-02"],
            "LONGITUDE": [10.5, np.nan],
            "LATITUDE": [np.nan, 20.5],
            "DEPTH": [100.0, np.nan],
        }
        pandas_df = ddf.from_pandas(pd.DataFrame(data))

        # Call the function
        result = _generate_partial_json_array(pandas_df, 10)

        # Parse the JSON result but need to get it back to object so that compare
        # of null in json string is converted back to None in object
        parsed_result = json.loads(_response_json(result, compress=False).body)

        # Expected output
        expected = [
            {
                "time": "2023-01-01",  # Adjust format based on _reformat_date
                "longitude": 10.5,
                "latitude": None,
                "depth": 100.0,
            },
            {
                "time": "2023-01-02",  # Adjust format based on _reformat_date
                "longitude": None,
                "latitude": 20.5,
                "depth": None,
            },
        ]

        # Verify that NaN values are converted to None (null in JSON)
        assert (
            parsed_result == expected
        ), f"Expected {expected}, but got {parsed_result}"

        # Additional checks for None values
        assert parsed_result[0]["latitude"] is None, "LATITUDE NaN should be None"
        assert parsed_result[1]["longitude"] is None, "LONGITUDE NaN should be None"
        assert parsed_result[1]["depth"] is None, "DEPTH NaN should be None"

    def test_generate_partial_json_array_maps_darwin_core_fields(self):
        # test generate partial json array with normalised name field
        df = ddf.from_pandas(
            pd.DataFrame(
                {
                    "eventDate": ["2016-02-07"],
                    "decimalLongitude": [147.123456],
                    "decimalLatitude": [-42.987654],
                }
            ),
            npartitions=1,
        )

        result = list(_generate_partial_json_array(df, None))

        self.assertEqual(
            result,
            [
                {
                    "time": "2016-02-07",
                    "longitude": 147.1,
                    "latitude": -43.0,
                }
            ],
        )

    def test_refresh_uuid_dataset_map_logs_dataset_failure_and_continues(self):
        api = API()
        api._metadata = MagicMock()
        api._metadata.catalog = {}
        api._metadata.metadata_catalog_uncached.return_value = {
            "bad_dataset.parquet": {},
            "good_dataset.parquet": {
                "dataset_metadata": {
                    "uuid": "good-uuid",
                }
            },
        }

        def get_metadata_uuid(data):
            if data == {}:
                # mock raised error
                raise ValueError("bad dataset metadata")
            return "good-uuid"

        with patch.object(API, "get_metadata_uuid", side_effect=get_metadata_uuid):
            with patch.object(api, "_extract_coordinate", return_value=None):
                with self.assertLogs(
                    "data_access_service.core.api", level="ERROR"
                ) as logs:
                    api.refresh_uuid_dataset_map()

        log_output = "\n".join(logs.output)

        self.assertIn(
            "Failed to refresh UUID dataset map for dataset=bad_dataset.parquet uuid=None",
            log_output,
        )
        self.assertIn("good-uuid", api._raw)
        self.assertIn("good_dataset.parquet", api._raw["good-uuid"])
        self.assertIn("good-uuid", api._cached_metadata)
        self.assertIn("good_dataset.parquet", api._cached_metadata["good-uuid"])

    def test_normalize_lon(self):
        """Test None"""
        self.assertEqual(BaseAPI.normalize_lon(None), None)
        self.assertAlmostEqual(BaseAPI.normalize_lon(-252.85), 107.15, delta=0.01)
        self.assertAlmostEqual(BaseAPI.normalize_lon(-209.96), 150.04, delta=0.01)

        """Test standard longitude values"""
        self.assertEqual(BaseAPI.normalize_lon(0), 0)
        self.assertEqual(BaseAPI.normalize_lon(90), 90)
        self.assertEqual(BaseAPI.normalize_lon(-90), -90)
        self.assertEqual(BaseAPI.normalize_lon(180), 180)
        self.assertEqual(BaseAPI.normalize_lon(-180), -180)

        """Test positive wrap-arounds (>180)"""
        self.assertEqual(BaseAPI.normalize_lon(181), -179)
        self.assertEqual(BaseAPI.normalize_lon(360), 0)
        self.assertEqual(
            BaseAPI.normalize_lon(540), 180
        )  # 180 = -180 due to circle !!! Issue??
        self.assertEqual(BaseAPI.normalize_lon(1000), -80)

        """Test negative wrap-arounds (<-180)"""
        self.assertEqual(BaseAPI.normalize_lon(-181), 179)
        self.assertEqual(BaseAPI.normalize_lon(-360), 0)
        self.assertEqual(BaseAPI.normalize_lon(-540), -180)
        self.assertEqual(BaseAPI.normalize_lon(-1000), 80)

        """Test exact boundary values"""
        self.assertEqual(BaseAPI.normalize_lon(180.0), 180.0)
        self.assertEqual(BaseAPI.normalize_lon(-180.0), -180.0)
        self.assertEqual(BaseAPI.normalize_lon(179.999), 179.999)
        self.assertEqual(BaseAPI.normalize_lon(-179.999), -179.999)

        """Test with decimal degrees"""
        self.assertAlmostEqual(BaseAPI.normalize_lon(181.5), -178.5)
        self.assertAlmostEqual(BaseAPI.normalize_lon(-181.5), 178.5)
        self.assertAlmostEqual(BaseAPI.normalize_lon(360.1), 0.1)

        """Test values from actual GPS datasets"""
        cases = [
            (370.0, 10.0),  # Pacific crossing
            (-350.0, 10.0),  # Atlantic crossing
            (179.999999, 179.999999),
            (-179.999999, -179.999999),
        ]
        for input_lon, expected in cases:
            with self.subTest(input_lon=input_lon):
                self.assertAlmostEqual(BaseAPI.normalize_lon(input_lon), expected)

    with open(
        Path(__file__).resolve().parent.parent / "canned/catalog_uncached.json", "r"
    ) as file:

        @patch.object(
            DataQuery.Metadata,
            "metadata_catalog_uncached",
            return_value=json.load(file),
        )
        def test_normalize_to_0_360_if_needed(self, get_metadata):
            """
            Data from satellite may use lon [0, 360] rather than the usual [-180. 180], this function is used to test
            the conversion is correct, the function check dataset metadata min max lon to confirm which range it belong
            :param get_metadata:
            :return:
            """
            api = API()
            api.initialize_metadata()

            uuid = "a4170ca8-0942-4d13-bdb8-ad4718ce14bb"
            key = "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr"

            """Test None"""
            self.assertEqual(api.normalize_to_0_360_if_needed(uuid, key, None), None)

            self.assertAlmostEqual(
                180, api.normalize_to_0_360_if_needed(uuid, key, 0), places=6
            )

            with self.assertRaises(TypeError):
                api.normalize_to_0_360_if_needed(uuid, key, 370)
                api.normalize_to_0_360_if_needed(uuid, key, -370.0)

    def test_get_temporal_extent_future_and_past_dates(self):
        api = API()
        uuid = "test-uuid"
        key = "test-key"

        mock_descriptor = MagicMock()
        mock_descriptor.dname = "test-dname"
        api._cached_metadata = {uuid: {key: mock_descriptor}}

        mock_ds = MagicMock()
        api._instance = MagicMock()
        api._instance.get_dataset.return_value = mock_ds

        # Case A: Naive end_date in the future
        future_naive = pd.Timestamp.now() + pd.Timedelta(days=5)
        start_naive = pd.Timestamp("2020-01-01 12:00:00")
        mock_ds.get_temporal_extent.return_value = (start_naive, future_naive)

        start_res, end_res = api.get_temporal_extent(uuid, key)
        self.assertEqual(start_res, pd.Timestamp("2020-01-01 00:00:00"))
        self.assertIsNotNone(end_res)
        self.assertLessEqual(end_res, pd.Timestamp.now())
        self.assertIsNone(end_res.tzinfo)

        # Case B: Aware end_date in the future
        future_aware = pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=5)
        start_aware = pd.Timestamp("2020-01-01 12:00:00", tz="UTC")
        mock_ds.get_temporal_extent.return_value = (start_aware, future_aware)

        start_res, end_res = api.get_temporal_extent(uuid, key)
        self.assertEqual(start_res, pd.Timestamp("2020-01-01 00:00:00", tz="UTC"))
        self.assertIsNotNone(end_res)
        self.assertLessEqual(end_res, pd.Timestamp.now(tz="UTC"))
        self.assertIsNotNone(end_res.tzinfo)

        # Case C: Naive end_date in the past
        past_naive = pd.Timestamp.now() - pd.Timedelta(days=5)
        mock_ds.get_temporal_extent.return_value = (start_naive, past_naive)

        start_res, end_res = api.get_temporal_extent(uuid, key)
        expected_end_naive = past_naive.replace(
            hour=23, minute=59, second=59, microsecond=999999, nanosecond=999
        )
        self.assertEqual(end_res, expected_end_naive)

        # Case D: Aware end_date in the past
        past_aware = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=5)
        mock_ds.get_temporal_extent.return_value = (start_aware, past_aware)

        start_res, end_res = api.get_temporal_extent(uuid, key)
        expected_end_aware = past_aware.tz_convert("UTC").replace(
            hour=23, minute=59, second=59, microsecond=999999, nanosecond=999
        )
        self.assertEqual(end_res, expected_end_aware)

        # Case E: Cached metadata does not exist
        start_res, end_res = api.get_temporal_extent("non-existent-uuid", key)
        self.assertIsNone(start_res)
        self.assertIsNone(end_res)
