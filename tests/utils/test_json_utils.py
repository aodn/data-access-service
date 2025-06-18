# test_json_utils.py
import unittest
from data_access_service.utils.json_utils import is_json_different, replace_json_values


class TestJsonUtils(unittest.TestCase):

    def test_is_json_different_with_differences(self):
        json_1 = {"key1": "value1", "key2": "value2"}
        json_2 = {"key1": "value1", "key2": "different_value"}
        ignored_files = []
        self.assertTrue(is_json_different(json_1, json_2, ignored_files))

    def test_is_json_different_with_ignored_fields(self):
        json_1 = {"key1": "value1", "key2": "value2"}
        json_2 = {"key1": "value1", "key2": "different_value"}
        ignored_files = ["root['key2']"]
        self.assertFalse(is_json_different(json_1, json_2, ignored_files))

    def test_is_json_different_no_differences(self):
        json_1 = {"key1": "value1", "key2": "value2"}
        json_2 = {"key1": "value1", "key2": "value2"}
        ignored_files = []
        self.assertFalse(is_json_different(json_1, json_2, ignored_files))

    def test_replace_json_values(self):
        json_data = {"key1": "value1", "key2": {"key3": "value3"}}
        replacements = {"key1": "new_value1", "key3": "new_value3"}
        expected_result = {"key1": "new_value1", "key2": {"key3": "new_value3"}}
        self.assertEqual(replace_json_values(json_data, replacements), expected_result)

    def test_replace_json_values_no_replacements(self):
        json_data = {"key1": "value1", "key2": {"key3": "value3"}}
        replacements = {"key4": "new_value4"}
        expected_result = {"key1": "value1", "key2": {"key3": "value3"}}
        self.assertEqual(replace_json_values(json_data, replacements), expected_result)
