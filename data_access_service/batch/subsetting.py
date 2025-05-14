import json

from data_access_service import init_log, Config
from data_access_service.batch.batch_enums import Parameters
from data_access_service.core.AWSClient import AWSClient
from data_access_service.tasks.generate_csv_file import process_data_files
from data_access_service.utils.date_time_utils import get_boundary_of_year_month, transfer_date_range_into_yearmonth, \
    split_yearmonths_into_dict

# we may need to change the divisor later according to cost or performance consideration
month_count_per_job = 6
#
# class ParamField(Enum):
#     UUID = "uuid"
#     START_DATE = "start_date"
#     END_DATE = "end_date"
#     MULTI_POLYGON = "multi_polygon"
#     RECIPIENT = "recipient"
#     DATE_RANGES = "date_ranges"


def prepare_data(job_id, job_index, parameters):
    logger = init_log(Config.get_config())
    # get params
    uuid = parameters[Parameters.UUID.value]
    date_ranges = parameters[Parameters.DATE_RANGES.value]
    date_ranges_dict = json.loads(date_ranges)
    logger.info("Date Ranges:", date_ranges_dict)
    child_year_month = date_ranges_dict[job_index]
    start_date, end_date = get_boundary_of_year_month(child_year_month)


    multi_polygon = parameters[Parameters.MULTI_POLYGON.value]
    recipient = parameters[Parameters.RECIPIENT.value]

    logger.info("UUID:", uuid)
    logger.info("Start Date:", start_date)
    logger.info("End Date:", end_date)
    logger.info("Multi Polygon:", multi_polygon)
    logger.info("Recipient:", recipient)

    process_data_files(job_id, uuid, start_date, end_date, multi_polygon, recipient)


def init(job_id, parameters):
    logger = init_log(Config.get_config())

    uuid = parameters[Parameters.UUID.value]
    start_date = parameters[Parameters.START_DATE.value]
    end_date = parameters[Parameters.END_DATE.value]
    year_months = transfer_date_range_into_yearmonth(start_date=start_date, end_date=end_date)
    month_count = len(year_months)
    array_job_count = month_count // month_count_per_job + 1
    date_ranges = split_yearmonths_into_dict(year_months, month_count)
    parameters[Parameters.DATE_RANGES.value] = json.dumps(date_ranges)
    # in parameters, change type into "sub-setting-data-preparation"
    parameters["type"] = "sub-setting-data-preparation"

    aws_client = AWSClient()

    # submit data preparation job

    preparation_parameters = {
        **parameters,
        Parameters.TYPE.value: "sub-setting-data-preparation",
    }
    data_preparation_job_id = aws_client.submit_a_job(
        job_name="prepare-data-for-job-" + job_id,
        job_queue="generate-csv-data-file",
        job_definition="generate-csv-data-file-dev",
        parameters=preparation_parameters,
        array_size=array_job_count,
        dependency_job_id=job_id,
    )

    #submit data collection job

    collection_parameters = {
        **parameters,
        Parameters.TYPE.value: "sub-setting-data-collection",
    }


    aws_client.submit_a_job(
        job_name="collect-data-for-job-" + job_id,
        job_queue="generate-csv-data-file",
        job_definition="generate-csv-data-file-dev",
        parameters=collection_parameters,
        dependency_job_id=data_preparation_job_id
    )


