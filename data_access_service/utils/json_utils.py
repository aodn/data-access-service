import io
import json
import os
import zipfile
from pathlib import Path

import boto3
from deepdiff import DeepDiff

from data_access_service import init_log
from data_access_service.config.config import Config



def is_json_different(remote_resource_json: dict, local_json_path: Path, ignored_added_fields: list[str]) -> bool:

    try:
        with open(local_json_path, 'r') as file:
            local_json = json.load(file)

        differences = DeepDiff(local_json, remote_resource_json, ignore_order=True)
        # Filter out ignored fields
        for addedItem in differences.get( "dictionary_item_added", []):
            if addedItem not in ignored_added_fields: # new field added
                return True

        if differences.get( "dictionary_item_removed", []): # field removed
            return True

        if differences.get("values_changed", {}): # field value changed
            return True

        return False
    except Exception as e:
        self.log.error(f"Error checking differences: {e}")
        raise e

