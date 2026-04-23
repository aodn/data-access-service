from pprint import pprint

from aodn_cloud_optimised.lib.DataQuery import Metadata, DataSource, GetAodn

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError
from data_access_service.models.CO_data_source.abstract_data_src import AbstractDataSrc


class AodnDataSrc(AbstractDataSrc):

    def __init__(self):
        self.name = "aodn"
        self.__data_src = GetAodn()
        self.__metadata = self.__data_src.get_metadata()
        self.__metadata_catalog = self.__metadata.catalog

    def get_metadata(self) -> Metadata:
        return self.__metadata

    def get_metadata_catalog(self) -> dict:
        return self.__metadata_catalog

    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        if dataset_name_with_ext not in self.__metadata_catalog:
            raise DatasetNotFoundError(
                dataset_name=dataset_name_with_ext, data_source_name=self.name
            )

        return self.__data_src.get_dataset(dataset_name_with_ext)


if __name__ == "__main__":
    aodn = AodnDataSrc()
    catalog = aodn.get_metadata_catalog()
    pprint(catalog)
