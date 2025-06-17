import copy
import json
from logging import Logger
from pathlib import Path

from deepdiff import DeepDiff


def is_json_different(json_1: dict, json_2: dict, ignored_files: list[str]) -> bool:

    differences = DeepDiff(json_1, json_2, ignore_order=True)

    # Filter out ignored fields
    for addedItem in differences.get( "dictionary_item_added", []):
        if addedItem not in ignored_files: # new field added
            return True

    for removedItem in differences.get( "dictionary_item_removed", []):
        if removedItem not in ignored_files:
            return True

    for changedItem in differences.get("values_changed", {}):
        if changedItem not in ignored_files:
            return True

    return False

def has_json_value_changed(json_data: dict, key: str, new_value: any) -> bool:

    """
    Check if a specific key's value in a JSON-like dictionary has changed.

    Args:
        json_data (dict): The JSON-like dictionary to check.
        key (str): The key whose value is to be checked.
        new_value: The new value to compare against the existing value.

    Returns:
        bool: True if the value has changed, False otherwise.
    """
    return json_data.get(key) != new_value

def replace_json_values(json_data: dict, replacements: dict) -> dict:
    """
    Recursively replace values in a JSON-like dictionary based on a replacement mapping.

    Args:
        json_data (dict): The JSON-like dictionary to modify.
        replacements (dict): A mapping of keys to their new values.

    Returns:
        dict: The modified JSON-like dictionary with replaced values.
    """
    json_data_copy = copy.deepcopy(json_data)
    for key, value in json_data_copy.items():
        if isinstance(value, dict):
            json_data_copy[key] = replace_json_values(value, replacements)
        elif key in replacements:
            json_data_copy[key] = replacements[key]
    return json_data_copy