import logging
import requests
from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource, GetAodn

from data_access_service.config.config import Config
from data_access_service.models.co_data_source.abstract_data_src import (
    AbstractDataSrc,
    CSIRO,
)

log = logging.getLogger(__name__)


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
        log.info(
            "Requesting temporary access keys for CSIRO dataset '%s'...", dataset_name
        )
        response = requests.get(key_request_url, timeout=30)
        log.info(
            "Received response for CSIRO dataset '%s', status code: %s",
            dataset_name,
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

            csiro = GetAodn(
                bucket_name=bucket_name,
                prefix=prefix,
                s3_fs_opts={
                    "key": access_key,
                    "secret": secret_access_key,
                    "client_kwargs": {"endpoint_url": endpoint_url},
                },
            )
            log.info(
                "Successfully initialized CSIRO data source for dataset '%s'",
                dataset_name,
            )
            return csiro
        else:
            raise Exception(
                f"Failed to get keys from CSIRO for dataset '{dataset_name}', status code: {response.status_code}"
            )
