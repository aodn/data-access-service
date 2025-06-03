from enum import Enum


class Parameters(Enum):
    UUID = "uuid"
    START_DATE = "start_date"
    END_DATE = "end_date"
    MULTI_POLYGON = "multi_polygon"
    RECIPIENT = "recipient"
    DATE_RANGES = "date_ranges"
    TYPE = "type"
    MASTER_JOB_ID = "master_job_id"

