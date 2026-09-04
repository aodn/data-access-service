"""CSIRO hosted cloud optimised datasets.

CSIRO keeps its parquet in its own bucket behind its own S3 endpoint, and the
keys to read it are short lived (about two days) and handed out by the
collection's key request URL. Two very different readers need that access, so
both live here:

* :class:`CsiroDataSrc` wraps it in a ``GetAodn`` handle for the API.
* :func:`locate_csiro_dataset` hands it to the batch jobs as a
  :class:`DatasetLocation`, which they turn into a DuckDB secret.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import requests
from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource, GetAodn

from data_access_service.config.config import Config
from data_access_service.models.co_data_source.abstract_data_src import (
    AbstractDataSrc,
    CSIRO,
)
from data_access_service.models.co_data_source.dataset_location import DatasetLocation

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

    def to_dataset_location(self) -> DatasetLocation:
        """The same access in the shape the batch jobs and DuckDB want."""
        endpoint = urlparse(self.endpoint_url)
        return DatasetLocation(
            bucket=self.bucket,
            prefix=self.prefix,
            # DuckDB's ENDPOINT takes the host on its own; the scheme becomes
            # USE_SSL.
            endpoint=endpoint.netloc or endpoint.path,
            use_ssl=endpoint.scheme != "http",
            access_key=self.access_key,
            secret_access_key=self.secret_access_key,
        )


def get_csiro_key_request_url(dataset_name: str) -> Optional[str]:
    """The configured key request URL, or None when the dataset is not CSIRO's."""
    for dataset in Config.get_config().get_csiro_datasets():
        if dataset.get("dataset_name") == dataset_name:
            return dataset.get("key_request_url")
    return None


def request_csiro_s3_access(dataset_name: str, key_request_url: str) -> CsiroS3Access:
    """Ask CSIRO for temporary keys for one dataset."""
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


class CsiroDataSrc(AbstractDataSrc):
    """
    Integrates with CSIRO cloud optimised datasets.
    Supports multiple datasets, each with its own key_request_url, as configured in the config YAML.
    """

    def __init__(self):
        self.name = CSIRO
        config = Config.get_config()
        self.__datasets = config.get_csiro_datasets()
        # dict of dataset_name -> GetAodn instance
        self.__data_srcs: dict[str, GetAodn] = {
            ds["dataset_name"]: self.__init_data_src(
                ds["dataset_name"], ds["key_request_url"]
            )
            for ds in self.__datasets
        }
        self.__metadata_catalog = self.__build_metadata_catalog()

    def get_metadata(self) -> Metadata:
        raise NotImplementedError(
            "get_metadata is not implemented yet. Currently the DataQuery.GetAodn.get_metadata() doesn't work"
        )

    def get_metadata_catalog(self) -> dict:
        return self.__metadata_catalog

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        return super().get_dataset(dataset_name_with_ext=dataset_name_with_ext)

    def get_name(self) -> str:
        return self.name

    def get_data_src(self) -> GetAodn:
        """Returns the hard-coded GetAodn instance for the UWY dataset as temp solution"""
        return self.__data_srcs["uwy_csiro.parquet"]

    @classmethod
    def locate_dataset(cls, dataset_name_with_ext: str) -> Optional[DatasetLocation]:
        """Where CSIRO keeps this dataset, or None when it is not theirs.

        Keys are requested per call because they expire: a long job that
        resolved once at start-up could find them dead by the time it reads.
        """
        key_request_url = get_csiro_key_request_url(dataset_name_with_ext)
        if key_request_url is None:
            return None
        return request_csiro_s3_access(
            dataset_name_with_ext, key_request_url
        ).to_dataset_location()

    def __build_metadata_catalog(self) -> dict:
        catalog = {}
        for dataset_name, data_src in self.__data_srcs.items():
            metadata = data_src.get_dataset(dataset_name).get_metadata()
            if not isinstance(metadata, dict):
                raise Exception(
                    f"Unexpected metadata format for CSIRO dataset {dataset_name}: {metadata}"
                )
            #  hardcode uuid until csiro add uuid into their global attributes in parquet
            if metadata.get("global_attributes") is not None:
                if metadata["global_attributes"].get("metadata_uuid") is None:
                    metadata["global_attributes"][
                        "metadata_uuid"
                    ] = "154a59da-b88a-4231-97df-c0407a6f0ec4"
            catalog[dataset_name] = metadata
        return catalog

    def __init_data_src(self, dataset_name: str, key_request_url: str) -> GetAodn:
        access = request_csiro_s3_access(dataset_name, key_request_url)
        csiro = GetAodn(
            bucket_name=access.bucket,
            prefix=access.prefix,
            s3_fs_opts=access.to_s3_fs_opts(),
        )
        log.info(
            "Successfully initialized CSIRO data source for dataset '%s'",
            dataset_name,
        )
        return csiro
