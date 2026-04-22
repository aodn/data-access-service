import logging

from aodn_cloud_optimised.lib.DataQuery import GetAodn, Metadata, DataSource

from data_access_service.models.CO_data_source.co_data_src_utils import (
    get_csiro_co_data_src,
)
from data_access_service.utils.common_utils import compare_dict_keys


class CODataRegistry:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.log.info("Initializing all Cloud Optimized data sources...")
        self.data_source_list: list[GetAodn] = [get_csiro_co_data_src(), GetAodn()]
        self.log.info("All Cloud Optimized data sources initialized")

    # since only catalog in DataQuery.Metadata is using by this project now, so only combine the catalogs for now.
    def get_metadata(self) -> Metadata:
        self.log.info("Getting metadata from all data sources...")

        # Just a temp solution for now since the GetAodn.get_metadata() doesn't work for external data source
        aodn = next(
            src for src in self.data_source_list if src.bucket_name != "dapprd-mnf"
        )
        self.log.info("Getting metadata from AODN data source...")
        metadata = aodn.get_metadata()
        csiro = next(
            src for src in self.data_source_list if src.bucket_name == "dapprd-mnf"
        )
        self.log.info("Getting metadata from CSIRO data source...")
        csiro_catalog = _get_catalog_helper(data_src=csiro)
        has_same_dataset_name, conflicted_names = compare_dict_keys(
            metadata.catalog, csiro_catalog
        )
        if has_same_dataset_name:
            raise Exception(
                f"Conflicted dataset names found in different data sources: {conflicted_names}. Please contact Data Uplift team"
            )

        metadata.catalog = metadata.catalog | csiro_catalog
        self.log.info("Metadata retrieved from all data source")
        return metadata

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        self.log.info(
            f"Getting dataset {dataset_name_with_ext} from all data sources..."
        )
        for data_src in self.data_source_list:
            try:
                return data_src.get_dataset(dataset_name_with_ext)
            except FileNotFoundError as e:
                # log the exception and continue to try the next data source
                self.log.info(
                    f"Dataset {dataset_name_with_ext} not found in data source {data_src}. Trying next data source"
                )
                continue

        raise Exception(f"Dataset {dataset_name_with_ext} not found in any data source")


def _get_catalog_helper(data_src: GetAodn):
    # for external data source, the GetAodn.get_metadata() doesn't work, so temp solution is using this helper to get metadata.
    if data_src.bucket_name == "dapprd-mnf":
        # Ensure catalog is a mapping of dataset name -> metadata dict
        single_md = data_src.get_dataset("uwy_csiro.parquet").get_metadata()
        if not isinstance(single_md, dict):
            raise Exception(
                f"Unexpected catalog format for CSIRO data source: {single_md}"
            )
        return single_md
    else:
        raise Exception(
            f"Unexpected data source for _get_catalog_helper: {data_src.bucket_name}"
        )
