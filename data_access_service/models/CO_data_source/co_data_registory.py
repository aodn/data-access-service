import logging

from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError
from data_access_service.models.CO_data_source.abstract_data_src import (
    AbstractDataSrc,
    AODN,
)
from data_access_service.models.CO_data_source.aodn_data_src import AodnDataSrc
from data_access_service.models.CO_data_source.csiro_data_src import CsiroDataSrc
from data_access_service.utils.common_utils import compare_dict_keys

log = logging.getLogger(__name__)


class CODataRegistry:
    def __init__(self):
        log.info("Initializing all Cloud Optimized data sources...")
        self.data_source_list: list[AbstractDataSrc] = [AodnDataSrc(), CsiroDataSrc()]
        log.info("All Cloud Optimized data sources initialized")

    # since only catalog in DataQuery.Metadata is using by this project now, so only combine the catalogs for now.
    def get_metadata(self) -> Metadata:
        log.info("Getting metadata from all data sources...")

        # Just a temp solution for now since the GetAodn.get_metadata() doesn't work for external data source
        aodn = next(src for src in self.data_source_list if src.get_name() == AODN)
        log.info("Getting metadata from AODN data source...")
        metadata = aodn.get_metadata()

        log.info("Getting metadata from external data sources ...")
        for src in self.data_source_list:
            if src.get_name() == AODN:
                continue

            log.info(f"Getting metadata catalog from data source {src.get_name()}...")
            catalog = src.get_metadata_catalog()
            has_same_dataset_name, conflicted_names = compare_dict_keys(
                metadata.catalog, catalog
            )
            if has_same_dataset_name:
                raise Exception(
                    f"Conflicted dataset names found in different data sources: {conflicted_names}. Please contact Data Uplift team"
                )
            metadata.catalog = metadata.catalog | catalog

        log.info("Metadata retrieved from all data source")
        return metadata

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        log.info(f"Getting dataset {dataset_name_with_ext} from all data sources...")
        for data_src in self.data_source_list:
            try:
                return data_src.get_dataset(dataset_name_with_ext)
            except DatasetNotFoundError as e:
                # log the exception and continue to try the next data source
                log.info(
                    f"Dataset {dataset_name_with_ext} not found in data source {data_src}. Trying next data source"
                )
                continue

        raise Exception(f"Dataset {dataset_name_with_ext} not found in any data source")
