from unittest.mock import patch
from aodn_cloud_optimised import DataQuery
from data_access_service import API

import unittest
import json
import os


class TestApi(unittest.TestCase):
    # Use this canned data as the metadata map
    with open(os.getcwd() + "/tests/canned/catalog_uncached.json", "r") as file:

        @patch.object(
            DataQuery.Metadata,
            "metadata_catalog_uncached",
            return_value=json.load(file),
        )
        def test_map_column_names(self, get_metadata):
            api = API()

            d = api.get_raw_meta_data("541d4f15-122a-443d-ab4e-2b5feb08d6a0")
            meta: dict = d.get("dataset_metadata")
            self.assertEqual(
                meta.get("title").casefold(),
                "AATAMS ACOUSTIC".casefold(),
                "Title equals",
            )

            # Now if you try to map the field, since this metadata do not have TIME, so it should return timestamp
            col = api.map_column_names(
                "541d4f15-122a-443d-ab4e-2b5feb08d6a0",
                ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(
                col, ["timestamp", "DEPTH", "LATITUDE", "LONGITUDE"], "TIME mapped"
            )

            # This uuid have time so it will not map
            col = api.map_column_names(
                "af5d0ff9-bb9c-4b7c-a63c-854a630b6984",
                ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(
                col,
                ["timestamp", "DEPTH", "LATITUDE", "LONGITUDE"],
                "TIME no need to map",
            )

            # This uuid have JULD but no time and timestamp, so map it to JULD
            col = api.map_column_names(
                "95d6314c-cfc7-40ae-b439-85f14541db71",
                ["TIME", "DEPTH", "LATITUDE", "LONGITUDE"],
            )
            self.assertListEqual(
                col, ["JULD", "DEPTH", "LATITUDE", "LONGITUDE"], "TIME mapped"
            )


if __name__ == "__main__":
    unittest.main()
