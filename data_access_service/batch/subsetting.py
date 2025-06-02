import json

from data_access_service import init_log, Config, API
from data_access_service.batch.batch_enums import Parameters
from data_access_service.core.AWSClient import AWSClient
from data_access_service.tasks.data_collection import collect_data_files
from data_access_service.tasks.generate_csv_file import process_data_files
from data_access_service.utils.date_time_utils import trim_date_range, \
    supply_day, split_date_range, parse_date

# we may need to change the divisor later according to cost or performance consideration
month_count_per_job = 3

def init(job_id_of_init, parameters):
    logger = init_log(Config.get_config())

    uuid = parameters[Parameters.UUID.value]
    start_date_str = parameters[Parameters.START_DATE.value]
    end_date_str = parameters[Parameters.END_DATE.value]

    requested_start_date, requested_end_date = supply_day(start_date_str, end_date_str)
    start_date, end_date = trim_date_range(api=API(), uuid=uuid, requested_start_date=requested_start_date,
                                           requested_end_date=requested_end_date)
    date_ranges = split_date_range(start_date=start_date, end_date=end_date,
                                   month_count_per_job=month_count_per_job)
    parameters[Parameters.DATE_RANGES.value] = json.dumps(date_ranges)

    aws_client = AWSClient()

    # submit data preparation job
    preparation_parameters = {
        **parameters,
        Parameters.TYPE.value: "sub-setting-data-preparation",
    }
    data_preparation_job_id = aws_client.submit_a_job(
        job_name="prepare-data-for-job-" + job_id_of_init,
        job_queue="generate-csv-data-file",
        job_definition="generate-csv-data-file-dev",
        parameters=preparation_parameters,
        array_size=len(date_ranges),
        dependency_job_id=job_id_of_init,
    )

    # submit data collection job
    collection_parameters = {
        **parameters,
        Parameters.TYPE.value: "sub-setting-data-collection",
    }

    aws_client.submit_a_job(
        job_name="collect-data-for-job-" + job_id_of_init,
        job_queue="generate-csv-data-file",
        job_definition="generate-csv-data-file-dev",
        parameters=collection_parameters,
        dependency_job_id=data_preparation_job_id
    )


def prepare_data(master_job_id, job_index, parameters):
    logger = init_log(Config.get_config())
    # get params
    uuid = parameters[Parameters.UUID.value]
    date_ranges = parameters[Parameters.DATE_RANGES.value]
    date_ranges_dict = json.loads(date_ranges)
    logger.info("Date Ranges:", date_ranges_dict)

    multi_polygon = parameters[Parameters.MULTI_POLYGON.value]

    logger.info("UUID:", uuid)
    logger.info("Multi Polygon:", multi_polygon)

    date_range = date_ranges_dict[str(job_index)]
    start_date = parse_date(date_range[0])
    end_date = parse_date(date_range[1])
    logger.info("Start Date:", start_date)
    logger.info("End Date:", end_date)

    process_data_files(master_job_id, uuid, start_date, end_date, multi_polygon)


def collect_data(master_job_id, parameters):
    recipient = parameters[Parameters.RECIPIENT.value]
    uuid = parameters[Parameters.UUID.value]

    collect_data_files(master_job_id=master_job_id, dataset_uuid=uuid, recipient=recipient)

















