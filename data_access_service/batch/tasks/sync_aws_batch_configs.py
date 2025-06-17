import json
from logging import Logger
from pathlib import Path

from data_access_service import Config, init_log
from data_access_service.batch.batch_stats import batch_json_settings_ignored_field
from data_access_service.core.AWSClient import AWSClient
from data_access_service.utils.json_utils import is_json_different, replace_json_values

job_definition_path = path = Path(__file__).parent.parent.parent / "config/batch/generatecsv/job_definition.json"
job_queue_path = Path(__file__).parent.parent.parent / "config/batch/generatecsv/job_queue.json"
compute_environment_path = Path(__file__).parent.parent.parent / "config/batch/generatecsv/compute_environment.json"

def sync_aws_batch_configs():
    config = Config.get_config()
    aws = AWSClient()
    log = init_log(config)

    log.info("Checking if batch job definition needs to be synced...")
    sync_aws_batch_job_definition(config=config, aws=aws, log=log)
    sync_aws_batch_compute_environment(config=config, aws=aws, log=log)
    sync_aws_batch_job_queue(config=config, aws=aws, log=log)




def sync_aws_batch_job_definition(config: Config, aws: AWSClient, log: Logger):
    job_definition_name = config.get_job_definition_name()
    cloud_job_definition = aws.get_batch_job_definition_config(job_definition_name)
    local_job_definition = get_local_json(config=config, path=job_definition_path)

    if cloud_job_definition is None:
        log.warning("No cloud job definition found. Registering new job definition: " + job_definition_name)
        log.warning("New job definition won't be registered for avoliding wrong ARN reference etc.. Please register it manually.")
        return

    if is_json_different(json_1=cloud_job_definition, json_2=local_job_definition, ignored_files=batch_json_settings_ignored_field):
        log.info("Cloud job definition is different from local. Registering new job definition: " + job_definition_name)
        aws.register_batch_job_definition(job_definition=local_job_definition)
        return

    log.info("Cloud job definition is the same as local. No need to register a new job definition: " + job_definition_name)

def sync_aws_batch_job_queue(config: Config, aws: AWSClient, log: Logger):
    job_queue_name = config.get_job_queue_name()
    cloud_job_queue = aws.get_batch_job_queue_config(job_queue_name)
    local_job_queue = get_local_json(config=config, path=job_queue_path)

    if cloud_job_queue is None:
        log.warning("No cloud job queue found. Registering new job queue: " + job_queue_name)
        log.warning("New job queue won't be registered for avoliding wrong ARN reference etc.. Please register it manually.")
        return

    # if is_json_different(json_1=cloud_job_queue, json_2=local_job_queue, ignored_files=batch_json_settings_ignored_field):
    if does_job_queue_need_update(cloud_job_queue=cloud_job_queue, local_job_queue=local_job_queue, log=log):
        log.info("Cloud job queue is different from local. Updating job queue: " + job_queue_name)
        aws.update_batch_job_queue(local_job_queue)
        return

    log.info("Cloud job queue is the same as local. No need to register a new job queue: " + job_queue_name)

def sync_aws_batch_compute_environment(config: Config, aws: AWSClient, log: Logger):
    compute_environment_name = config.get_compute_environment_name()
    cloud_compute_environment = aws.get_batch_compute_environment_config(compute_environment_name)
    local_compute_environment = get_local_json(config=config, path=compute_environment_path)

    if cloud_compute_environment is None:
        log.warning("No cloud compute environment found. Registering new compute environment: " + compute_environment_name)
        log.warning("New compute environment won't be registered for avoliding wrong ARN reference etc.. Please register it manually.")
        return

    # if is_json_different(json_1=cloud_compute_environment, json_2=local_compute_environment, ignored_files=batch_json_settings_ignored_field):
    if does_compute_environment_need_update(cloud_compute_environment=cloud_compute_environment, local_compute_environment=local_compute_environment, log=log):
        log.info("Cloud compute environment is different from local. Updating compute environment: " + compute_environment_name)
        aws.update_batch_compute_environment(local_compute_environment)
        return

    log.info("Cloud compute environment is the same as local. No need to register a new compute environment: " + compute_environment_name)

def get_local_json (config: Config, path: Path) -> dict:
    with open(path, 'r') as file:
        local_job_definition = json.load(file)

    # update the job definition according to the environment
    return replace_json_values(json_data=local_job_definition, replacements=config.get_job_definition_distinct_fields())

def does_job_queue_need_update(cloud_job_queue: dict, local_job_queue: dict, log: Logger) -> bool:

    for key, value in local_job_queue.items():

        # When updating a job queue, we need to use "jobQueue" to instead "jobQueueName"
        if key == "jobQueue":
            if value != cloud_job_queue["jobQueueName"]:
                raise ValueError(
                    f"Job queue name mismatch: local {value} vs remote {cloud_job_queue['jobQueueName']}"
                )
        elif isinstance(value, dict):
            if is_json_different(value, cloud_job_queue.get(key, {}), ignored_files=[]):
                log.info(f"Compute environment needs update")
                return True
        elif value != cloud_job_queue.get(key):
            log.info(f"Job queue needs update")
            return True

    return False

def does_compute_environment_need_update(cloud_compute_environment: dict, local_compute_environment: dict, log: Logger) -> bool:

    for key, value in local_compute_environment.items():


        # When updating a compute environment, we need to use "computeEnvironment" to instead "computeEnvironmentName"
        if key == "computeEnvironment":
            if value != cloud_compute_environment["computeEnvironmentName"]:
                raise ValueError(
                    f"Compute environment name mismatch: local {value} vs remote {cloud_compute_environment['computeEnvironmentName']}"
                )
        elif isinstance(value, dict):
            if is_json_different(value, cloud_compute_environment.get(key, {}), ignored_files=["root['type']"]):
                log.info(f"Compute environment needs update")
                return True
        elif value != cloud_compute_environment.get(key):
            log.info(f"Compute environment needs update")
            return True

    return False