import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from es_aws_functions import exception_classes
from moto import mock_sns, mock_sqs
from pandas.util.testing import assert_frame_equal

import calculate_imputation_factors_method
import calculate_imputation_factors_wrangler


class MockContext:
    aws_request_id = 666


mock_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "movement_type": "movement_calculation_b",
    "period": 201809,
    "run_id": "example",
    "queue_url": "Earl",
    "distinct_values": ["strata", "region"],
    "period_column": "period",
    "incoming_message_group_id": "I am GROOP",
    "in_file_name": "Test",
    "location": "Here",
    "outgoing_message_group_id": "mock_message",
    "out_file_name": "Test",
    "factors_parameters":
    {
        "factors_type": "factors_calculation_a",
        "percentage_movement": True,
        "survey_column": "survey",
        "region_column": "region",
        "regionless_code": 14,
        "first_imputation_factor": 1,
        "second_imputation_factor": 2,
        "first_threshold": 7,
        "second_threshold": 7,
        "third_threshold": 9,
        "regional_mean": "third_imputation_factors"
    },
    "questions_list": ["Q601_asphalting_sand",
                       "Q602_building_soft_sand",
                       "Q603_concreting_sand",
                       "Q604_bituminous_gravel",
                       "Q605_concreting_gravel",
                       "Q606_other_gravel",
                       "Q607_constructional_fill"],
    "sns_topic_arn": "mock_arn"
  }
}

context_object = MockContext


class TestWranglerAndMethod(unittest.TestCase):
    """
    Tests the wrangler and method lambdas integration.
    """

    @classmethod
    def setup_class(cls):
        """
        sets up the mock boto clients and starts the patchers.
        :return: None.

        """
        # setting up the wrangler mock boto patcher and starting the patch
        cls.mock_boto_wrangler_patcher = mock.patch(
            "calculate_imputation_factors_wrangler.boto3"
        )
        cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()

        # setting up the mock environment variables for the wrangler
        cls.mock_os_wrangler_patcher = mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "method_name": "mock_method",
                "period_column": "mock_period",
                "run_environment": "development",
                "bucket_name": "Mike"
            },
        )

        # setting up the mock environment variables for the method
        cls.mock_os_method_patcher = mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "run_environment": "development",
                "period": "mock_period"
            },
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()
        cls.mock_os_m = cls.mock_os_method_patcher.start()

    @classmethod
    def teardown_class(cls):
        """
        stops the wrangler, method and os patchers.
        :return: None.

        """
        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_os_wrangler_patcher.stop()
        cls.mock_os_method_patcher.stop()

    @mock_sns
    @mock_sqs
    def test_wrangler(self):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        # load json file.

        method_input_file = (
            "tests/fixtures/calculate_imputation_factors_method_input_data_a.json"
        )

        with open(method_input_file, "r") as file:
            json_content = file.read()

        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ,
                {"sqs_queue_url": sqs_queue_url}
        ):
            with mock.patch(
                    "calculate_imputation_factors_wrangler.aws_functions"
            ) as funk:
                funk.get_dataframe.return_value = \
                    pd.DataFrame(json.loads(json_content)), 777

                with mock.patch(
                    "calculate_imputation_factors_wrangler.boto3.client"
                ) as mocked:
                    mocked_client = mock.Mock()
                    mocked.return_value = mocked_client

                    with open(method_input_file, "r") as file:
                        mocked_client.invoke.return_value \
                            .get.return_value.read \
                            .return_value.decode.return_value = json.dumps({
                                "data": file.read(), "success": True
                            })

                        out = calculate_imputation_factors_wrangler.lambda_handler(
                            mock_event, context_object
                        )
                        assert "success" in out
                        assert out["success"]

    @mock_sqs
    @mock_sns
    def test_method(self):
        """
        mocks functionality of the method
        :return: None
        """
        # ----------- Test the 'A' path -----------
        # read in the json payload from wrangler
        input_file = "tests/fixtures/"
        input_file += "calculate_imputation_factors_method_input_data_a.json"

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        output_file = calculate_imputation_factors_method.lambda_handler(
            {
                "data_json": json_content,
                "questions_list": ["Q601_asphalting_sand",
                                   "Q602_building_soft_sand",
                                   "Q603_concreting_sand",
                                   "Q604_bituminous_gravel",
                                   "Q605_concreting_gravel",
                                   "Q606_other_gravel",
                                   "Q607_constructional_fill"],
                "distinct_values": ["strata", "region", "period"],
                "factors_parameters": {
                    "RuntimeVariables": {
                        "factors_type": "factors_calculation_a",
                        "percentage_movement": True,
                        "survey_column": "survey",
                        "region_column": "region",
                        "regionless_code": 14,
                        "first_imputation_factor": 1,
                        "second_imputation_factor": 2,
                        "first_threshold": 7,
                        "second_threshold": 7,
                        "third_threshold": 9,
                        "regional_mean": "third_imputation_factors"
                    }
                },
                "sns_topic_arn": "mock_arn"
            }, context_object
        )

        # final output match
        expected_output_file = (
            "tests/fixtures/calculate_imputation_factors_method_output_a.json"
        )

        with open(expected_output_file, "r") as file:
            expected_dataframe = json.loads(file.read())

        actual_outcome_dataframe = pd.DataFrame(json.loads(output_file["data"]))
        expected_output_dataframe = pd.DataFrame(expected_dataframe)

        assert_frame_equal(
            actual_outcome_dataframe.astype(str), expected_output_dataframe.astype(str)
        )

        # ----------- Test the 'B' path -----------
        # read in the json payload from wrangler
        input_file = "tests/fixtures/"
        input_file += "calculate_imputation_factors_method_input_data_b.json"

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        output_file = calculate_imputation_factors_method.lambda_handler(
            {"data_json": json_content, 
                "questions_list": ["Commons_prod",
                                    "Commons_Dels",
                                    "Commons_C-stock"],
                "distinct_values": ["strata", "region"],
                "factors_parameters": {
                    "RuntimeVariables": {
                        "factors_type": "factors_calculation_b",
                        "survey_column": "survey",
                        "threshold": 7,
                    }},
                "sns_topic_arn": "mock_arn"
             }, context_object
        )

        # final output match
        expected_output_file = (
            "tests/fixtures/calculate_imputation_factors_method_output_b.json"
        )

        with open(expected_output_file, "r") as file:
            expected_dataframe = json.loads(file.read())

        actual_outcome_dataframe = pd.DataFrame(json.loads(output_file["data"]))
        expected_output_dataframe = pd.DataFrame(expected_dataframe)

        assert_frame_equal(
            actual_outcome_dataframe.astype(str), expected_output_dataframe.astype(str)
        )

    def test_method_exception_handling(self):
        """
        testing the exception handler within the method.
        :param self:
        :return: mock response
        """

        json_data_content = (
            '[{"movement_Q601_asphalting_sand":0.0},'
            '{"movement_Q601_asphalting_sand":0.857614899}]'
        )

        response = calculate_imputation_factors_method.lambda_handler(
            {"data_json": json_data_content,
             "questions_list": ["Q601_asphalting_sand",
                                "Q602_building_soft_sand",
                                "Q603_concreting_sand",
                                "Q604_bituminous_gravel",
                                "Q605_concreting_gravel",
                                "Q606_other_gravel",
                                "Q607_constructional_fill"]
             }, context_object
        )
        assert not response["success"]

    def test_raise_general_exception_wrangles(self):
        with mock.patch("calculate_imputation_factors_wrangler.boto3.client") as mocked:
            mocked.side_effect = Exception("AARRRRGHH!!")
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_imputation_factors_wrangler.lambda_handler(
                    {"RuntimeVariables": {"checkpoint": 666,
                                          "run_id": "bob",
                                          "queue_url": "Earl"}},
                    context_object
                )
            assert "AARRRRGHH" in exc_info.exception.error_message

    def test_raise_general_exception_method(self):
        with mock.patch("pandas.DataFrame") as mocked:
            mocked.side_effect = Exception("AARRRRGHH!!")
            json_data_content = (
                '[{"movement_Q601_asphalting_sand":0.0},'
                '{"movement_Q601_asphalting_sand":0.857614899}]'
            )
            response = calculate_imputation_factors_method.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666},
                 "data_json": json_data_content,
                 "distinct_values": ["strata", "region"],
                 "questions_list": ["Q601_asphalting_sand",
                                    "Q602_building_soft_sand",
                                    "Q603_concreting_sand",
                                    "Q604_bituminous_gravel",
                                    "Q605_concreting_gravel",
                                    "Q606_other_gravel",
                                    "Q607_constructional_fill"],
                 "factors_parameters": {
                        "RuntimeVariables": {
                            "factors_type": "factors_calculation_a",
                            "percentage_movement": True,
                            "survey_column": "survey",
                            "region_column": "region",
                            "regionless_code": 14,
                            "first_imputation_factor": 1,
                            "second_imputation_factor": 2,
                            "first_threshold": 7,
                            "second_threshold": 9,
                            "third_threshold": 9,
                            "regional_mean": "third_imputation_factors"
                        }
                    }
                 }, context_object
            )
            assert "success" in response
            assert response["success"] is False
            assert """AARRRRGHH!!""" in response["error"]

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ,
                {"sqs_queue_url": sqs_queue_url}
        ):
            calculate_imputation_factors_wrangler.os.environ.pop("method_name")
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_imputation_factors_wrangler.lambda_handler(
                    {"RuntimeVariables": {"checkpoint": 666,
                                          "run_id": "bob",
                                          "queue_url": "Earl"}},
                    context_object
                )
            assert "Parameter validation error" in exc_info.exception.error_message

    def test_method_key_error_exception(self):
        """
        mocks functionality of the method
        :return: None
        """
        # read in the json payload from wrangler
        input_file = "tests/fixtures/calculate_imputation_factors_method_input_data_a.json" # noqa

        with open(input_file, "r") as file:
            content = file.read()
            content = content.replace("Q", "MIKE")
            json_content = json.loads(content)

        output_file = calculate_imputation_factors_method.lambda_handler(
            {"data_json": json_content, "questions_list": ["Q601_asphalting_sand",
                                                           "Q602_building_soft_sand",
                                                           "Q603_concreting_sand",
                                                           "Q604_bituminous_gravel",
                                                           "Q605_concreting_gravel",
                                                           "Q606_other_gravel",
                                                           "Q607_constructional_fill"]
             }, context_object
        )

        assert not output_file["success"]
        assert "Key Error" in output_file["error"]

    @mock_sns
    @mock_sqs
    def test_wrangler_incomplete_read_error(self):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        # load json file.

        method_input_file = (
            "tests/fixtures/calculate_imputation_factors_method_input_data_a.json"
        )

        with open(method_input_file, "r") as file:
            json_content = file.read()

        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ,
                {"sqs_queue_url": sqs_queue_url}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                with mock.patch(
                    "calculate_imputation_factors_wrangler.aws_functions.get_dataframe"
                ) as get_data:
                    mocked_client = mock.Mock()
                    mocked.return_value = mocked_client
                    get_data.return_value = pd.DataFrame(json.loads(json_content)), 567

                    with open(method_input_file, "rb") as file:
                        invoke_return = file

                        mocked_client.invoke.return_value = {
                            "Payload": StreamingBody(invoke_return, 666)
                        }
                        with unittest.TestCase.assertRaises(
                                self, exception_classes.LambdaFailure) as exc_info:
                            calculate_imputation_factors_wrangler.lambda_handler(
                                mock_event, context_object
                            )
                        assert "Incomplete Lambda response" in \
                               exc_info.exception.error_message

    @mock_sns
    @mock_sqs
    def test_wrangler_key_error(self):
        with mock.patch(
            "calculate_imputation_factors_wrangler.aws_functions.get_dataframe"
        ) as mocked:
            mocked.side_effect = KeyError()
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_imputation_factors_wrangler.lambda_handler(
                    mock_event, context_object
                )
            assert "Key Error" in exc_info.exception.error_message

    def test_wrangler_client_error(self):
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ, {"sqs_queue_url": "mike"}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                mocked.side_effect = ClientError(
                    {"Error": {"Code": "Mike"}}, "create_stream"
                )
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    calculate_imputation_factors_wrangler.lambda_handler(
                        mock_event, context_object
                    )
                assert "AWS Error" in exc_info.exception.error_message

    @mock_sns
    @mock_sqs
    def test_wrangler_method_fail(self):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        # load json file.

        method_input_file = (
            "tests/fixtures/calculate_imputation_factors_method_input_data_a.json"
        )

        with open(method_input_file, "r") as file:
            json_content = file.read()

        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ,
                {"sqs_queue_url": sqs_queue_url}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                with mock.patch(
                    "calculate_imputation_factors_wrangler.aws_functions.get_dataframe"
                ) as funk_get_dataframe:
                    mocked_client = mock.Mock()
                    mocked.return_value = mocked_client
                    funk_get_dataframe.return_value = \
                        pd.DataFrame(json.loads(json_content)), 777

                    mocked_client.invoke.return_value.get.return_value \
                        .read.return_value.decode.return_value = \
                        json.dumps({"success": False,
                                    "error": "This is an error message"})
                    with unittest.TestCase.assertRaises(
                            self, exception_classes.LambdaFailure) as exc_info:
                        calculate_imputation_factors_wrangler.lambda_handler(
                            mock_event, context_object
                        )
                    assert "error message" in exc_info.exception.error_message
