import pandas as pd
import pytest
import os

from unittest.mock import patch, call
from data_access_service import API
from data_access_service.batch.subsetting import init
from data_access_service.config.config import EnvType, Config
from data_access_service.core.AWSHelper import AWSHelper
from tests.batch.batch_test_consts import (
    INIT_JOB_ID,
    INIT_PARAMETERS,
    PREPARATION_JOB_SUBMISSION_ARGS,
    COLLECTION_JOB_SUBMISSION_ARGS,
)


class TestInit:

    @pytest.fixture(autouse=True, scope="class")
    def setup(self):
        """Set environment variable for testing profile."""
        os.environ["PROFILE"] = EnvType.TESTING.value
        yield

    def test_init(self, setup):
        with patch.object(Config, "get_month_count_per_job") as get_month_count_per_job:
            with patch.object(AWSHelper, "submit_a_job") as submit_a_job:
                get_month_count_per_job.return_value = 3
                submit_a_job.return_value = "test-job-id-returned"

                # Call the init function
                init(INIT_JOB_ID, INIT_PARAMETERS)

                expected_call_1 = call(
                    job_name=PREPARATION_JOB_SUBMISSION_ARGS["job_name"],
                    job_queue=PREPARATION_JOB_SUBMISSION_ARGS["job_queue"],
                    job_definition=PREPARATION_JOB_SUBMISSION_ARGS["job_definition"],
                    parameters=PREPARATION_JOB_SUBMISSION_ARGS["parameters"],
                    array_size=PREPARATION_JOB_SUBMISSION_ARGS["array_size"],
                    dependency_job_id=INIT_JOB_ID,
                )

                expected_call_2 = call(
                    job_name=COLLECTION_JOB_SUBMISSION_ARGS["job_name"],
                    job_queue=COLLECTION_JOB_SUBMISSION_ARGS["job_queue"],
                    job_definition=COLLECTION_JOB_SUBMISSION_ARGS["job_definition"],
                    parameters=COLLECTION_JOB_SUBMISSION_ARGS["parameters"],
                    dependency_job_id=COLLECTION_JOB_SUBMISSION_ARGS[
                        "dependency_job_id"
                    ],
                )

                # Assert that submit_a_job was called twice
                assert submit_a_job.call_count == 2
                # Assert that the expected calls were made
                assert (
                    expected_call_1 == submit_a_job.call_args_list[0]
                ), "call arg list 1"
                assert (
                    expected_call_2 == submit_a_job.call_args_list[1]
                ), "call arg list 2"

    def test_init_with_very_narrow_date_range(self, setup):
        with patch.object(Config, "get_month_count_per_job") as get_month_count_per_job:
            with patch.object(API, "get_temporal_extent") as get_temporal_extent:
                with patch.object(AWSHelper, "submit_a_job") as submit_a_job:
                    get_month_count_per_job.return_value = 1200  # Set a very high month count to ensure no splitting occurs
                    # Mock the get_temporal_extent method to return a fixed value
                    get_temporal_extent.return_value = (
                        pd.Timestamp(year=1970, month=1, day=1),
                        pd.Timestamp(year=2024, month=12, day=31),
                    )

                    submit_a_job.return_value = "test-job-id-returned"

                    # Call the init function
                    init(INIT_JOB_ID, INIT_PARAMETERS)

                    expected_call_1 = call(
                        job_name=PREPARATION_JOB_SUBMISSION_ARGS["job_name"],
                        job_queue=PREPARATION_JOB_SUBMISSION_ARGS["job_queue"],
                        job_definition=PREPARATION_JOB_SUBMISSION_ARGS[
                            "job_definition"
                        ],
                        # parameters=PREPARATION_JOB_SUBMISSION_ARGS["parameters"],
                        parameters={
                            **PREPARATION_JOB_SUBMISSION_ARGS["parameters"],
                            "date_ranges": '{"0": ["2010-02-01 00:00:00.000000000", "2011-04-30 23:59:59.999999999"]}',
                        },
                        array_size=1,
                        dependency_job_id=INIT_JOB_ID,
                    )

                    expected_call_2 = call(
                        job_name=COLLECTION_JOB_SUBMISSION_ARGS["job_name"],
                        job_queue=COLLECTION_JOB_SUBMISSION_ARGS["job_queue"],
                        job_definition=COLLECTION_JOB_SUBMISSION_ARGS["job_definition"],
                        parameters={
                            **COLLECTION_JOB_SUBMISSION_ARGS["parameters"],
                            "date_ranges": '{"0": ["2010-02-01 00:00:00.000000000", "2011-04-30 23:59:59.999999999"]}',
                        },
                        dependency_job_id=COLLECTION_JOB_SUBMISSION_ARGS[
                            "dependency_job_id"
                        ],
                    )

                    # Assert that submit_a_job was called twice
                    assert submit_a_job.call_count == 2
                    # Assert that the expected calls were made
                    assert expected_call_1 in submit_a_job.call_args_list
                    assert expected_call_2 in submit_a_job.call_args_list
