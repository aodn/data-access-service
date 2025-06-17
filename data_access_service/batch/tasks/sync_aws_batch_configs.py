import json
from logging import Logger
from pathlib import Path

from data_access_service import Config, init_log
from data_access_service.batch.batch_stats import batch_job_definition_ignored_fields
from data_access_service.core.AWSClient import AWSClient
from data_access_service.utils.json_utils import is_json_different, replace_json_values


def sync_aws_batch_configs():
    config = Config.get_config()
    aws = AWSClient()
    log = init_log(config)

    log.info("Checking if batch job definition needs to be synced...")
    sync_aws_batch_job_definition(config=config, aws=aws, log=log)



def sync_aws_batch_job_definition(config: Config, aws: AWSClient, log: Logger):
    job_definition_name = config.get_job_definition_name()
    cloud_job_definition = aws.get_batch_job_definition_config(job_definition_name)
    local_job_definition = get_local_job_definition(config=config)

    if cloud_job_definition is None:
        log.warning("No cloud job definition found. Registering new job definition: " + job_definition_name)
        log.warning("New job definition won't be registered for avoliding wrong ARN reference etc.. Please register it manually.")
        return

    if is_json_different(json_1=cloud_job_definition, json_2=local_job_definition, ignored_files=batch_job_definition_ignored_fields):
        log.info("Cloud job definition is different from local. Registering new job definition: " + job_definition_name)
        aws.register_batch_job_definition(job_definition=local_job_definition)
        return

    log.info("Cloud job definition is the same as local. No need to register a new job definition: " + job_definition_name)



def get_local_job_definition (config: Config) -> dict:
    path = Path(__file__).parent.parent.parent / "config/batch/generatecsv/job_definition.json"
    with open(path, 'r') as file:
        local_job_definition = json.load(file)

    # update the job definition according to the environment
    return replace_json_values(json_data=local_job_definition, replacements=config.get_job_definition_distinct_fields())