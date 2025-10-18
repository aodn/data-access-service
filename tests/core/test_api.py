import json
import unittest
from typing import Dict, Any

import dask.dataframe as ddf
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch

from aodn_cloud_optimised import DataQuery

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

    def test_normalize_lon(self):
        """Test None"""
        self.assertEqual(BaseAPI.normalize_lon(None), None)

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
                10, api.normalize_to_0_360_if_needed(uuid, key, 370), places=6
            )
            self.assertAlmostEqual(
                350, api.normalize_to_0_360_if_needed(uuid, key, 350.0), places=6
            )
            self.assertAlmostEqual(
                350, api.normalize_to_0_360_if_needed(uuid, key, -370.0), places=6
            )
            self.assertAlmostEqual(
                180, api.normalize_to_0_360_if_needed(uuid, key, 540.0), places=6
            )
            self.assertAlmostEqual(
                179, api.normalize_to_0_360_if_needed(uuid, key, -181.0), places=6
            )
            self.assertAlmostEqual(
                181, api.normalize_to_0_360_if_needed(uuid, key, 181.0), places=6
            )
            self.assertAlmostEqual(
                180, api.normalize_to_0_360_if_needed(uuid, key, 0), places=6
            )
