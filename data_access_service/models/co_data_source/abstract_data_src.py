from abc import ABC, abstractmethod

from aodn_cloud_optimised.lib.DataQuery import DataSource, Metadata, GetAodn

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError

AODN = "aodn"
CSIRO = "csiro"


class AbstractDataSrc(ABC):

    @abstractmethod
    def get_metadata(self) -> Metadata:
        pass

    @abstractmethod
    def get_metadata_catalog(self) -> dict:
        pass

    @abstractmethod
    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        if dataset_name_with_ext not in self.get_metadata_catalog():
            raise DatasetNotFoundError(
                dataset_name=dataset_name_with_ext, data_source_name=self.get_name()
            )

        return self.get_data_src().get_dataset(dataset_name_with_ext)

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_data_src(self) -> GetAodn:
        pass
