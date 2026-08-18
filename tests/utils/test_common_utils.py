import unittest

from data_access_service.utils.common_utils import compare_dict_keys


class TestCommonUtils(unittest.TestCase):

    def test_compare_dict_keys(self):

        dict1 = {"a": 1, "b": 2, "c": 3}
        dict2 = {"b": 4, "c": 5, "d": 6}
        dict3 = {"e": 7, "f": 8}

        # Test with common keys
        has_common, common_keys = compare_dict_keys(dict1, dict2)
        self.assertTrue(has_common)
        self.assertEqual(common_keys, {"b", "c"})

        # Test with no common keys
        has_common, common_keys = compare_dict_keys(dict1, dict3)
        self.assertFalse(has_common)
        self.assertEqual(common_keys, set())
