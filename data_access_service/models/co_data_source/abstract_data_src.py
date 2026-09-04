from abc import ABC, abstractmethod
from typing import Optional

from aodn_cloud_optimised.lib.DataQuery import DataSource, Metadata, GetAodn

from data_access_service.exceptions.dataset_not_found_error import DatasetNotFoundError
from data_access_service.models.co_data_source.dataset_location import DatasetLocation

AODN = "aodn"
CSIRO = "csiro"


class AbstractDataSrc(ABC):

    @abstractmethod
    def get_metadata(self) -> Metadata:
        pass

    @abstractmethod
    def get_metadata_catalog(self) -> dict:
        pass

    def get_dataset_names(self) -> frozenset[str]:
        """Names of the datasets held by this source, e.g. {"argo.parquet"}.

        Only used to check whether a name exists, so a source with an expensive
        catalog should override this with a cheaper listing.
        """
        return frozenset(self.get_metadata_catalog())

    @abstractmethod
    def get_dataset(self, dataset_name_with_ext: str) -> DataSource:
        if dataset_name_with_ext not in self.get_dataset_names():
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

    @classmethod
    @abstractmethod
    def locate_dataset(cls, dataset_name_with_ext: str) -> Optional[DatasetLocation]:
        pass
