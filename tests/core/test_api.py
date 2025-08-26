import json
import unittest
from typing import Dict, Any

import dask.dataframe as ddf
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch

from aodn_cloud_optimised import DataQuery
from aodn_cloud_optimised.lib.DataQuery import GetAodn

from data_access_service import API
from data_access_service.core.api import  _extract_longitude
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

# TODO: not finished yet
def test_extract_latitude():
    """
    Test the extraction of latitude from a DataFrame.

    """
    aodn = GetAodn()
    # catalog = aodn.get_metadata().catalog
    path = Path(__file__).parent.parent / "canned/input_json/raw_metadata.json"
    with open(path, "r") as f:
        catalog = json.load(f)

    for key, value in catalog.items():
        lon = _extract_longitude(value)
        spatial_extent = aodn.get_dataset(key).get_spatial_extent()
        # print(f"spatial extent lons: {spatial_extent[1]}, {spatial_extent[3]}")
        print("spatial extent", spatial_extent)
        if not lon:
            print(f"Longitude not found in {key}")
        else:
            print(f"Longitude: {lon.valid_min} ~ {lon.valid_max} for key: {key}")



if __name__ == "__main__":
    unittest.main()
