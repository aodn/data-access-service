from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource, GetAodn

from data_access_service.models.co_data_source.abstract_data_src import (
    AbstractDataSrc,
    AODN,
)


class AodnDataSrc(AbstractDataSrc):

    def __init__(self):
        self.name = AODN
        self.__data_src = GetAodn()
        self.__dataset_names: frozenset[str] | None = None

    def get_name(self) -> str:
        return self.name

    def get_metadata(self) -> Metadata:
        return self.__data_src.get_metadata()

    def get_metadata_catalog(self) -> dict:
        return self.__data_src.get_metadata().catalog

    def get_dataset_names(self) -> frozenset[str]:
        if self.__dataset_names is None:
            self.__dataset_names = frozenset(self.__data_src.list_datasets())
        return self.__dataset_names

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        return super().get_dataset(dataset_name_with_ext=dataset_name_with_ext)

    def get_data_src(self) -> GetAodn:
        return self.__data_src
