from enum import Enum


class Parameters(Enum):
    UUID = "uuid"
    KEY = "key"
    START_DATE = "start_date"
    END_DATE = "end_date"
    MULTI_POLYGON = "multi_polygon"
    RECIPIENT = "recipient"
    DATE_RANGES = "date_ranges"
    TYPE = "type"
    MASTER_JOB_ID = "master_job_id"
    INTERMEDIATE_OUTPUT_FOLDER = "intermediate_output_folder"
    INDEX_DATASETS = "index_datasets"
