import unittest
from unittest.mock import patch
from data_access_service.tasks.sync_aws_batch_configs import (
    sync_aws_batch_job_definition,
    sync_aws_batch_job_queue,
    sync_aws_batch_compute_environment,
)


class TestSyncAWSBatchConfigs(unittest.TestCase):

    @patch("data_access_service.tasks.sync_aws_batch_configs.AWSClient")
    @patch("data_access_service.tasks.sync_aws_batch_configs.Config")
    @patch("data_access_service.tasks.sync_aws_batch_configs.init_log")
    def test_sync_aws_batch_job_definition_no_changes(
        self, mock_log, mock_config, mock_aws_client
    ):
        mock_aws = mock_aws_client.return_value
        mock_config_instance = mock_config.return_value
        mock_log_instance = mock_log.return_value

        mock_aws.get_batch_job_definition_config.return_value = {"key": "value"}
        mock_config_instance.get_job_definition_name.return_value = (
            "test-job-definition"
        )
        with patch(
            "data_access_service.tasks.sync_aws_batch_configs.get_local_json"
        ) as mock_get_local_json:
            mock_get_local_json.return_value = {"key": "value"}

            sync_aws_batch_job_definition(
                config=mock_config_instance, aws=mock_aws, log=mock_log_instance
            )

            mock_log_instance.info.assert_called_with(
                "Cloud job definition is the same as local. No need to register a new job definition: test-job-definition"
            )

    @patch("data_access_service.tasks.sync_aws_batch_configs.AWSClient")
    @patch("data_access_service.tasks.sync_aws_batch_configs.Config")
    @patch("data_access_service.tasks.sync_aws_batch_configs.init_log")
    def test_sync_aws_batch_job_queue_needs_update(
        self, mock_log, mock_config, mock_aws_client
    ):
        mock_aws = mock_aws_client.return_value
        mock_config_instance = mock_config.return_value
        mock_log_instance = mock_log.return_value

        mock_aws.get_batch_job_queue_config.return_value = {"key": "old_value"}
        mock_config_instance.get_job_queue_name.return_value = "test-job-queue"
        with patch(
            "data_access_service.tasks.sync_aws_batch_configs.get_local_json"
        ) as mock_get_local_json:
            mock_get_local_json.return_value = {"key": "new_value"}
            with patch(
                "data_access_service.tasks.sync_aws_batch_configs.does_job_queue_need_update"
            ) as mock_needs_update:
                mock_needs_update.return_value = True

                sync_aws_batch_job_queue(
                    config=mock_config_instance, aws=mock_aws, log=mock_log_instance
                )

                mock_log_instance.info.assert_called_with(
                    "Cloud job queue is different from local. Updating job queue: test-job-queue"
                )
                mock_aws.update_batch_job_queue.assert_called_once()

    @patch("data_access_service.tasks.sync_aws_batch_configs.AWSClient")
    @patch("data_access_service.tasks.sync_aws_batch_configs.Config")
    @patch("data_access_service.tasks.sync_aws_batch_configs.init_log")
    def test_sync_aws_batch_compute_environment_no_cloud_environment(
        self, mock_log, mock_config, mock_aws_client
    ):
        mock_aws = mock_aws_client.return_value
        mock_config_instance = mock_config.return_value
        mock_log_instance = mock_log.return_value

        mock_aws.get_batch_compute_environment_config.return_value = None
        mock_config_instance.get_compute_environment_name.return_value = (
            "test-compute-environment"
        )

        sync_aws_batch_compute_environment(
            config=mock_config_instance, aws=mock_aws, log=mock_log_instance
        )

        mock_log_instance.warning.assert_called_with(
            "New compute environment won't be registered for avoliding wrong ARN reference etc.. Please register it manually."
        )
