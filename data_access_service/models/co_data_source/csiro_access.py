"""Temporary S3 access for CSIRO hosted cloud optimised datasets.

CSIRO keeps its parquet in its own bucket behind its own S3 endpoint, and the
keys to read it are short lived (about two days) and handed out by the
collection's key request URL. Both readers need this - the DataQuery handle the
API uses, and the DuckDB scans the batch jobs use - so the request lives here
instead of inside one of them.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import requests

from data_access_service.config.config import Config

log = logging.getLogger(__name__)

# CSIRO puts the parquet under a "data/" folder inside the collection folder.
_DATA_FOLDER = "data/"

_REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class CsiroS3Access:
    """Where one CSIRO dataset lives, plus the temporary keys to read it."""

    bucket: str
    # Path of the dataset's parent folder inside the bucket, ending with "/",
    # e.g. "000072626v001/data/".
    prefix: str
    # Endpoint with scheme, e.g. "https://s3.data.csiro.au".
    endpoint_url: str
    access_key: str
    secret_access_key: str

    def to_s3_fs_opts(self) -> dict:
        """The same access as s3fs-style options, which is what GetAodn takes."""
        return {
            "key": self.access_key,
            "secret": self.secret_access_key,
            "client_kwargs": {"endpoint_url": self.endpoint_url},
        }


def get_csiro_key_request_url(dataset_name: str) -> Optional[str]:
    """The configured key request URL, or None when the dataset is not CSIRO's."""
    for dataset in Config.get_config().get_csiro_datasets():
        if dataset.get("dataset_name") == dataset_name:
            return dataset.get("key_request_url")
    return None


def request_csiro_s3_access(dataset_name: str, key_request_url: str) -> CsiroS3Access:
    """Ask CSIRO for temporary keys for one dataset.

    Callers request keys when they are about to read rather than reusing an
    older set, because the keys expire.
    """
    log.info("Requesting temporary access keys for CSIRO dataset '%s'...", dataset_name)
    response = requests.get(key_request_url, timeout=_REQUEST_TIMEOUT_SECONDS)
    log.info(
        "Received response for CSIRO dataset '%s', status code: %s",
        dataset_name,
        response.status_code,
    )
    if response.status_code != 200:
        raise Exception(
            f"Failed to get keys from CSIRO for dataset '{dataset_name}', "
            f"status code: {response.status_code}"
        )

    res = response.json()
    bucket = res["bucket"]
    remote_directory = res["remoteDirectory"]
    if not remote_directory.startswith(bucket + "/"):
        raise Exception(f"Unexpected remote directory format: {remote_directory}")

    # The trailing slash is not guaranteed, so strip and re-add it rather than
    # gluing "data/" straight onto the collection folder name.
    collection_dir = remote_directory[len(bucket) + 1 :].strip("/")
    access = CsiroS3Access(
        bucket=bucket,
        prefix=f"{collection_dir}/{_DATA_FOLDER}" if collection_dir else _DATA_FOLDER,
        endpoint_url=res["endPointUrl"],
        access_key=res["accessKey"],
        secret_access_key=res["secretAccessKey"],
    )
    log.info(
        "CSIRO dataset '%s' resolved to s3://%s/%s",
        dataset_name,
        access.bucket,
        access.prefix,
    )
    return access
