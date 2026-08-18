class DatasetNotFoundError(Exception):
    def __init__(self, dataset_name: str, data_source_name: str):
        super().__init__(
            f"Dataset: '{dataset_name}' not found in data source: '{data_source_name}'."
        )
        self.dataset_name = dataset_name
