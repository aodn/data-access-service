import json

from data_access_service import init_log, Config, API
from data_access_service.batch.batch_enums import Parameters
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.tasks.data_collection import collect_data_files
from data_access_service.tasks.generate_dataset import process_data_files
from data_access_service.utils.date_time_utils import (
    supply_day,
    split_date_range,
    parse_date,
)


def _get_key(parameters) -> list[str]:
    if (
        Parameters.KEY.value in parameters
        and parameters[Parameters.KEY.value] is not None
    ):
        return [item.strip() for item in parameters[Parameters.KEY.value].split(",")]
    else:
        return ["*"]


def _get_uuid(parameters) -> str:
    return parameters[Parameters.UUID.value]


# The only purpose is to create suitable number of child job, we can fine tune the value
# on what is optimal value later
def init(job_id_of_init, parameters):
    # Must be here, this give change for test to init properly before calling get_config()
    config = Config.get_config()

    month_count_per_job = config.get_month_count_per_job()
    start_date_str = parameters[Parameters.START_DATE.value]
    end_date_str = parameters[Parameters.END_DATE.value]

    requested_start_date, requested_end_date = supply_day(start_date_str, end_date_str)
    date_ranges = split_date_range(
        start_date=requested_start_date,
        end_date=requested_end_date,
        month_count_per_job=month_count_per_job,
    )

    aws_client = AWSHelper()

    # submit data preparation job
    preparation_parameters = {
        **parameters,
        Parameters.MASTER_JOB_ID.value: job_id_of_init,
        Parameters.TYPE.value: "sub-setting-data-preparation",
        Parameters.DATE_RANGES.value: json.dumps(date_ranges),
        Parameters.INTERMEDIATE_OUTPUT_FOLDER.value: config.get_temp_folder(
            job_id_of_init
        ),
    }
    data_preparation_job_id = aws_client.submit_a_job(
        job_name="prepare-data-for-job-" + job_id_of_init,
        job_queue=config.get_job_queue_name(),
        job_definition=config.get_job_definition_name(),
        parameters=preparation_parameters,
        array_size=len(date_ranges),
        dependency_job_id=job_id_of_init,
    )

    # submit data collection job
    collection_parameters = {
        **preparation_parameters,
        Parameters.TYPE.value: "sub-setting-data-collection",
    }

    aws_client.submit_a_job(
        job_name="collect-data-for-job-" + job_id_of_init,
        job_queue=config.get_job_queue_name(),
        job_definition=config.get_job_definition_name(),
        parameters=collection_parameters,
        dependency_job_id=data_preparation_job_id,
    )


def prepare_data(job_index: str | None, parameters):
    config = Config.get_config()
    logger = init_log(config)

    # get params
    uuid = _get_uuid(parameters)
    # An uuid can host multiple data file, so we need a key
    # to narrow our target file. Right now it will be the filename
    # if nothing specified, assume all files taken
    key = _get_key(parameters)
    master_job_id = parameters[Parameters.MASTER_JOB_ID.value]
    date_ranges = parameters[Parameters.DATE_RANGES.value]
    date_ranges_dict = json.loads(date_ranges)
    multi_polygon = parameters[Parameters.MULTI_POLYGON.value]
    intermediate_output_folder = parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value]

    if job_index is None:
        job_index = 0

    start_date_str, end_date_str = date_ranges_dict[str(job_index)]
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)

    logger.info(f"UUID: {uuid}")
    logger.info(f"KEY: {key}")
    logger.info(f"Date Ranges: {date_ranges_dict}")
    logger.info(f"Multi Polygon: {multi_polygon}")
    logger.info(f"Start Date:{start_date}")
    logger.info(f"End Date:{end_date}")

    process_data_files(
        master_job_id,
        job_index,
        intermediate_output_folder,
        uuid,
        key,
        start_date,
        end_date,
        multi_polygon,
    )


def collect_data(parameters):
    recipient = parameters[Parameters.RECIPIENT.value]
    uuid = parameters[Parameters.UUID.value]
    master_job_id = parameters[Parameters.MASTER_JOB_ID.value]

    collect_data_files(
        master_job_id=master_job_id, dataset_uuid=uuid, recipient=recipient
    )
