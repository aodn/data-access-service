import logging
import requests
from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource, GetAodn

from data_access_service.models.CO_data_source.abstract_data_src import (
    AbstractDataSrc,
    CSIRO,
)

log = logging.getLogger(__name__)


class CsiroDataSrc(AbstractDataSrc):
    """
    Currently, this "Marine National Facility (MNF) Processed Underway Parquet Data" is the only external
    cloud optimized dataset we are integrating with, so there are a lot of hard coded values and assumptions in the code.
    It needs refactoring when there are more external cloud optimized datasets.
    """

    # TODO: this is a temp name to remind us to that currently we only integrate one dataset from csiro.
    #  please refactor when there are more datasets.
    THE_ONLY_DATASET_NAME = "uwy_csiro.parquet"
    # this one is only for dataset "Marine National Facility (MNF) Processed Underway Parquet Data"
    CSIRO_KEY_REQUEST_URL = (
        "https://data.csiro.au/dap/api/v2/collections/72626/files/s3"
    )

    def __init__(self):
        self.name = CSIRO
        self.__data_src = self.__get_csiro_co_data_src()
        self.__metadata_catalog = self.__get_csiro_co_dataset_catalog()

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
        return self.__data_src

    def __get_csiro_co_dataset_catalog(self) -> dict:
        single_metadata = self.__data_src.get_dataset(
            self.THE_ONLY_DATASET_NAME
        ).get_metadata()
        if not isinstance(single_metadata, dict):
            raise Exception(
                f"Unexpected metadata format for CSIRO dataset {self.THE_ONLY_DATASET_NAME}: {single_metadata}"
            )

        #  hardcode uuid until csiro add uuid into their global attributes in parquet
        # if single_metadata.get("global_attributes") is not None:
        #     if single_metadata["global_attributes"].get("metadata_uuid") is None:
        #         single_metadata["global_attributes"]["metadata_uuid"] = "154a59da-b88a-4231-97df-c0407a6f0ec4"
        return {self.THE_ONLY_DATASET_NAME: single_metadata}

    def __get_csiro_co_data_src(self) -> GetAodn:

        log.info(
            "Requesting temporary access keys for CSIRO cloud optimized dataset..."
        )
        response = requests.get(self.CSIRO_KEY_REQUEST_URL, timeout=10)
        log.info(
            "Received response from CSIRO for temporary access keys, status code: %s",
            response.status_code,
        )
        if response.status_code == 200:
            res = response.json()
            access_key = res["accessKey"]
            secret_access_key = res["secretAccessKey"]
            endpoint_url = res["endPointUrl"]
            bucket_name = res["bucket"]
            remote_directory = res["remoteDirectory"]
            if remote_directory.startswith(bucket_name + "/"):
                prefix = remote_directory[len(bucket_name) + 1 :] + "data/"
            else:
                raise Exception(
                    f"Unexpected remote directory format: {remote_directory}"
                )

            params = {
                "bucket_name": bucket_name,
                "prefix": prefix,
                "key": access_key,
                "secret": secret_access_key,
                "endpoint_url": endpoint_url,
            }
            csiro = GetAodn(
                bucket_name=params["bucket_name"],
                prefix=params["prefix"],
                s3_fs_opts={
                    "key": params["key"],
                    "secret": params["secret"],
                    "client_kwargs": {"endpoint_url": params["endpoint_url"]},
                },
            )
            log.info(
                "Successfully initialized CSIRO cloud optimized data source with temporary access keys"
            )
            return csiro

        else:
            raise Exception(
                f"Failed to get keys from CSIRO, status code: {response.status_code}"
            )
