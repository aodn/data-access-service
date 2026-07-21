import json

from data_access_service import config, init_log, Config, API
from data_access_service.batch.subsetting.enums import Parameters
from data_access_service.batch.subsetting.helpers.request_helper import (
    get_subset_request,
)
from data_access_service.core.AWSHelper import AWSHelper
from data_access_service.utils.subset_request_resolver import (
    ResolvedSubsetRequest,
    normalize_request,
    resolve_subset_request,
)
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.batch.subsetting.tasks.data_collection import (
    collect_data_files,
)
from data_access_service.batch.subsetting.tasks.generate_dataset import (
    process_data_files,
)
from data_access_service.batch.subsetting.tasks.zarr_processor import ZarrProcessor
from data_access_service.utils.date_time_utils import (
    split_date_range,
    parse_date,
)


# The only purpose is to create suitable number of child job, we can fine tune the value
# on what is optimal value later
def init(api: API, job_id_of_init, parameters):
    # Must be here, this give change for test to init properly before calling get_config()
    config = Config.get_config()

    # Step 1: Parse the raw parameters to SubsetRequest object
    parsed_subset_request = get_subset_request(parameters)

    # Step 2: Normalize the request which downstream jobs and result emails read these fields:
    #  1. Expand "*" keys
    #  2. Resolve "non-specified" dates to defaults
    subset_request = normalize_request(api, parsed_subset_request)

    # Step 3: Resolve the normalized request into what will actually be
    # executed:
    #  1. Trim the date range to the dataset's temporal extent
    #  2. Parse the GeoJSON multi_polygon into bboxes
    resolved_subset_request = resolve_subset_request(
        api=api,
        uuid=subset_request.uuid,
        keys=subset_request.keys,
        start_date_str=subset_request.start_date,
        end_date_str=subset_request.end_date,
        multi_polygon=subset_request.multi_polygon,
        bboxes=subset_request.bboxes,  # already parsed in get_subset_request; reuse
    )

    # Step 4: If the requested data not available, email the user and stop here.
    if not resolved_subset_request.has_data:
        text_body = (
            f"No data available for your subset request for dataset {subset_request.uuid} "
            f"with keys {subset_request.keys} "
            f"and date range from {subset_request.start_date} to {subset_request.end_date}."
            f"and selected area is {subset_request.multi_polygon}."
        )
        AWSHelper().send_email(
            recipient=subset_request.recipient,
            subject="No Data Available for Your Subset Request",
            text_body=text_body,
        )
        return

    # Step 5: Process zarr sub-setting workflow
    # use new zarr sub-setting workflow if all keys are zarr. It it works well, then deprecate old zarr sub-setting workflow
    if all(key.endswith(".zarr") for key in resolved_subset_request.keys):
        run_zarr_subset(api, job_id_of_init, subset_request, resolved_subset_request)
        return

    # Step 6: Process legacy sub-setting workflow, for parquet (or mixed
    # zarr+parquet)
    #  1. Split the date range into chunks
    #  2. Submit the data preparation job
    #  3. Submit the data collection job
    month_count_per_job = config.get_month_count_per_job()
    date_ranges = split_date_range(
        start_date=resolved_subset_request.start_date,
        end_date=resolved_subset_request.end_date,
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


def prepare_data(api: API, job_index: str | None, parameters) -> str | None:
    config = Config.get_config()
    logger = init_log(config)

    # get params
    request = get_subset_request(parameters)
    master_job_id = parameters[Parameters.MASTER_JOB_ID.value]
    date_ranges = parameters[Parameters.DATE_RANGES.value]
    date_ranges_dict = json.loads(date_ranges)
    intermediate_output_folder = parameters[Parameters.INTERMEDIATE_OUTPUT_FOLDER.value]

    if job_index is None:
        job_index = 0

    start_date_str, end_date_str = date_ranges_dict[str(job_index)]
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)

    logger.info(
        f"""
    ==============================
    UUID: {request.uuid}
    KEY: {request.keys}
    Date Ranges: {date_ranges_dict}
    Multi Polygon: {request.multi_polygon}
    Start Date:{start_date}
    End Date:{end_date}
    ==============================
    """
    )

    return process_data_files(
        api,
        master_job_id,
        job_index,
        intermediate_output_folder,
        request,
        start_date,
        end_date,
    )


def collect_data(parameters):
    master_job_id = parameters[Parameters.MASTER_JOB_ID.value]
    request = get_subset_request(parameters)
    collect_data_files(master_job_id=master_job_id, subset_request=request)


def run_zarr_subset(
    api: API, job_id: str, request: SubsetRequest, resolved: ResolvedSubsetRequest
) -> None:
    """Run the zarr-only subset pipeline. `resolved` must have data
    (the caller sends the "no data" email when it does not)."""
    ZarrProcessor(
        api=api, job_id=job_id, subset_request=request, resolved=resolved
    ).process()
