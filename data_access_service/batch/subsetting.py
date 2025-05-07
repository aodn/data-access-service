import os
from enum import Enum

from data_access_service import init_log, Config
from data_access_service.config.config import EnvType
from data_access_service.tasks.generate_csv_file import process_csv_data_file


class ParamField(Enum):
    UUID = "uuid"
    START_DATE = "start_date"
    END_DATE = "end_date"
    MULTI_POLYGON = "multi_polygon"
    RECIPIENT = "recipient"


def execute(job_id, parameters):
    logger = init_log(Config.get_config())
    # get params
    uuid = parameters[ParamField.UUID.value]
    start_date = parameters[ParamField.START_DATE.value]
    end_date = parameters[ParamField.END_DATE.value]
    multi_polygon = parameters[ParamField.MULTI_POLYGON.value]
    recipient = parameters[ParamField.RECIPIENT.value]

    logger.info("UUID:", uuid)
    logger.info("Start Date:", start_date)
    logger.info("End Date:", end_date)
    logger.info("Multi Polygon:", multi_polygon)
    logger.info("Recipient:", recipient)

    process_csv_data_file(job_id, uuid, start_date, end_date, multi_polygon, recipient)
