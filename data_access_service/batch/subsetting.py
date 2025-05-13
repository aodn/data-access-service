import json
from enum import Enum

from data_access_service import init_log, Config
from data_access_service.tasks.generate_csv_file import process_data_files
from data_access_service.utils.date_time_utils import get_boundary_of_year_month, transfer_date_range_into_yearmonth

# we may need to change the divisor later according to cost or performance consideration
month_count_per_job = 6

class ParamField(Enum):
    UUID = "uuid"
    START_DATE = "start_date"
    END_DATE = "end_date"
    MULTI_POLYGON = "multi_polygon"
    RECIPIENT = "recipient"
    DATE_RANGES = "date_ranges"


def execute(job_id, job_index, parameters):
    logger = init_log(Config.get_config())
    # get params
    uuid = parameters[ParamField.UUID.value]
    # start_date = parameters[ParamField.START_DATE.value]
    # end_date = parameters[ParamField.END_DATE.value]
    date_ranges = parameters[ParamField.DATE_RANGES.value]
    date_ranges_dict = json.loads(date_ranges)
    logger.info("Date Ranges:", date_ranges_dict)
    child_year_month = date_ranges_dict[job_index]
    start_date, end_date = get_boundary_of_year_month(child_year_month)


    multi_polygon = parameters[ParamField.MULTI_POLYGON.value]
    recipient = parameters[ParamField.RECIPIENT.value]

    logger.info("UUID:", uuid)
    logger.info("Start Date:", start_date)
    logger.info("End Date:", end_date)
    logger.info("Multi Polygon:", multi_polygon)
    logger.info("Recipient:", recipient)

    process_data_files(job_id, uuid, start_date, end_date, multi_polygon, recipient)


def init(job_id, parameters):
    logger = init_log(Config.get_config())

    uuid = parameters[ParamField.UUID.value]
    start_date = parameters[ParamField.START_DATE.value]
    end_date = parameters[ParamField.END_DATE.value]
    year_months = transfer_date_range_into_yearmonth(start_date=start_date, end_date=end_date)
    month_count = len(year_months)
    array_job_count = month_count // month_count_per_job + 1


