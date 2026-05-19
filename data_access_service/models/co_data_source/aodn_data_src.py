from pprint import pprint

from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource, GetAodn

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError
from data_access_service.models.co_data_source.abstract_data_src import (
    AbstractDataSrc,
    AODN,
)


class AodnDataSrc(AbstractDataSrc):

    def __init__(self):
        self.name = AODN
        self.__data_src = GetAodn()
        self.__metadata = self.__data_src.get_metadata()
        self.__metadata_catalog = self.__metadata.catalog

    def get_name(self) -> str:
        return self.name

    def get_metadata(self) -> Metadata:
        return self.__metadata

    def get_metadata_catalog(self) -> dict:
        return self.__metadata_catalog

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        return super().get_dataset(dataset_name_with_ext=dataset_name_with_ext)

    def get_data_src(self) -> GetAodn:
        return self.__data_src
