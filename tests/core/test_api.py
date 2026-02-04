import json
import unittest
from typing import Dict, Any

import dask.dataframe as ddf
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock

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

    def test_fetch_wave_buoy_latest_date(self):
        api = API()
        mock_df = pd.DataFrame({"TIME": [pd.Timestamp("2025-01-15 12:30:00")]})
        api.memconn = MagicMock()
        api.memconn.execute.return_value.df.return_value = mock_df

        result = api.fetch_wave_buoy_latest_date()

        self.assertEqual(result, "2025-01-15")
        api.memconn.execute.assert_called_once()

    def test_fetch_wave_buoy_sites(self):
        api = API()
        mock_df = pd.DataFrame(
            {
                "site_name": ["Brisbane", "Sydney"],
                "TIME": [
                    pd.Timestamp("2025-01-10 08:00:00"),
                    pd.Timestamp("2025-01-11 09:00:00"),
                ],
                "LATITUDE": [-27.47, -33.87],
                "LONGITUDE": [153.03, 151.21],
            }
        )
        api.memconn = MagicMock()
        api.memconn.execute.return_value.df.return_value = mock_df

        result = api.fetch_wave_buoy_sites("2025-01-10", "2025-01-12")

        self.assertEqual(result["type"], "FeatureCollection")
        self.assertEqual(len(result["features"]), 2)

        feature0 = result["features"][0]
        self.assertEqual(feature0["type"], "Feature")
        self.assertEqual(feature0["properties"]["buoy"], "Brisbane")
        self.assertEqual(feature0["properties"]["date"], "2025-01-10")
        self.assertEqual(feature0["geometry"]["type"], "Point")
        self.assertEqual(feature0["geometry"]["coordinates"], [153.03, -27.47])

        feature1 = result["features"][1]
        self.assertEqual(feature1["properties"]["buoy"], "Sydney")
        self.assertEqual(feature1["properties"]["date"], "2025-01-11")
        self.assertEqual(feature1["geometry"]["coordinates"], [151.21, -33.87])

    def test_fetch_wave_buoy_data(self):
        api = API()
        position_df = pd.DataFrame(
            {"LATITUDE": [-27.47], "LONGITUDE": [153.03]}
        )
        data_df = pd.DataFrame(
            {
                "TIME": [
                    pd.Timestamp("2025-01-10 08:00:00"),
                    pd.Timestamp("2025-01-10 09:00:00"),
                ],
                "SSWMD": [180.0, np.nan],
                "WPFM": [0.08, 0.09],
                "WPMH": [np.nan, 5.0],
                "WHTH": [1.2, 1.3],
                "WSSH": [np.nan, np.nan],
            }
        )
        api.memconn = MagicMock()
        api.memconn.execute.return_value.df.side_effect = [position_df, data_df]

        result = api.fetch_wave_buoy_data("Brisbane", "2025-01-10", "2025-01-11")

        self.assertEqual(result["type"], "Feature")
        self.assertEqual(result["geometry"]["coordinates"], [153.03, -27.47])

        # SSWMD: first row has value, second is NaN
        self.assertEqual(len(result["properties"]["SSWMD"]), 1)
        self.assertAlmostEqual(result["properties"]["SSWMD"][0][1], 180.0)

        # WPFM: both rows have values
        self.assertEqual(len(result["properties"]["WPFM"]), 2)

        # WPMH: only second row has value
        self.assertEqual(len(result["properties"]["WPMH"]), 1)
        self.assertAlmostEqual(result["properties"]["WPMH"][0][1], 5.0)

        # WHTH: both rows have values
        self.assertEqual(len(result["properties"]["WHTH"]), 2)

        # WSSH: both NaN, so empty
        self.assertEqual(len(result["properties"]["WSSH"]), 0)


