import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from moto import mock_sns, mock_sqs
from pandas.util.testing import assert_frame_equal

import calculate_imputation_factors_method
import calculate_imputation_factors_wrangler


class MockContext:
    aws_request_id = 666


mock_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "calculation_type": "movement_calculation_b",
    "period": 201809,
    "id": "example",
    "distinct_values": "region",
    "period_column": "period"
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
                "sns_topic_arn": "mock_arn",
                "checkpoint": "mock_checkpoint",
                "method_name": "mock_method",
                "period_column": "mock_period",
                "questions_list": "Q601_asphalting_sand,"
                + "Q602_building_soft_sand,"
                + "Q603_concreting_sand,"
                + "Q604_bituminous_gravel,"
                + "Q605_concreting_gravel,"
                + "Q606_other_gravel,"
                + "Q607_constructional_fill",
                "sqs_queue_url": "mock_queue",
                "sqs_message_group_id": "mock_message",
                "incoming_message_group": "I am GROOP",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "bucket_name": "Mike"
            },
        )

        # setting up the mock environment variables for the method
        cls.mock_os_method_patcher = mock.patch.dict(
            "os.environ",
            {
                "sns_topic_arn": "mock_arn",
                "checkpoint": "mock_checkpoint",
                "first_imputation_factor": str(1),
                "first_threshold": str(7),
                "period": "mock_period",
                "questions_list": "Q601_asphalting_sand,"
                + "Q602_building_soft_sand,"
                + "Q603_concreting_sand,"
                + "Q604_bituminous_gravel,"
                + "Q605_concreting_gravel,"
                + "Q606_other_gravel,"
                + "Q607_constructional_fill",
                "sqs_queue_url": "mock_queue",
                "second_imputation_factor": str(2),
                "second_threshold": str(7),
                "third_imputation_factor": str(1),
                "third_threshold": str(9),
                "region_column": "region"
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
            "tests/fixtures/calculate_imputation_factors_method_input_data.json"
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
        # read in the json payload from wrangler
        input_file = "tests/fixtures/"
        input_file += "calculate_imputation_factors_method_input_data.json"

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        output_file = calculate_imputation_factors_method.lambda_handler(
            {"data_json": json_content, "questions_list": "Q601_asphalting_sand,"
                                                          + "Q602_building_soft_sand,"
                                                          + "Q603_concreting_sand,"
                                                          + "Q604_bituminous_gravel,"
                                                          + "Q605_concreting_gravel,"
                                                          + "Q606_other_gravel,"
                                                          + "Q607_constructional_fill"
             }, context_object
        )

        # final output match
        expected_output_file = (
            "tests/fixtures/calculate_imputation_factors_method_output.json"
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
             "questions_list": "Q601_asphalting_sand,"
             + "Q602_building_soft_sand,"
             + "Q603_concreting_sand,"
             + "Q604_bituminous_gravel,"
             + "Q605_concreting_gravel,"
             + "Q606_other_gravel,"
             + "Q607_constructional_fill"
             }, context_object
        )
        assert not response["success"]

    def test_raise_general_exception_wrangles(self):
        with mock.patch("calculate_imputation_factors_wrangler.boto3.client") as mocked:
            mocked.side_effect = Exception("AARRRRGHH!!")
            response = calculate_imputation_factors_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, context_object
            )
            assert "success" in response
            assert response["success"] is False
            assert """AARRRRGHH!!""" in response["error"]

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
                 "questions_list": "Q601_asphalting_sand,"
                 + "Q602_building_soft_sand,"
                 + "Q603_concreting_sand,"
                 + "Q604_bituminous_gravel,"
                 + "Q605_concreting_gravel,"
                 + "Q606_other_gravel,"
                 + "Q607_constructional_fill"
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
            out = calculate_imputation_factors_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, context_object
            )
            self.assertRaises(ValueError)
            assert """Parameter validation error""" in out["error"]

    @mock_sqs
    def test_marshmallow_raises_method_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        json_data_content = (
            '[{"movement_Q601_asphalting_sand":0.0},'
            '{"movement_Q601_asphalting_sand":0.857614899}]'
        )
        with mock.patch.dict(
            calculate_imputation_factors_method.os.environ,
                {"sqs_queue_url": sqs_queue_url}
        ):
            calculate_imputation_factors_method.os.environ.pop("first_threshold")
            out = calculate_imputation_factors_method.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666},
                 "data_json": json_data_content,
                 "questions_list": "Q601_asphalting_sand,"
                                   + "Q602_building_soft_sand,"
                                   + "Q603_concreting_sand,"
                                   + "Q604_bituminous_gravel,"
                                   + "Q605_concreting_gravel,"
                                   + "Q606_other_gravel,"
                                   + "Q607_constructional_fill"
                 }, context_object
            )
            self.assertRaises(ValueError)

            assert """Parameter validation error""" in out["error"]

    def test_method_key_error_exception(self):
        """
        mocks functionality of the method
        :return: None
        """
        # read in the json payload from wrangler
        input_file = "tests/fixtures/calculate_imputation_factors_method_input_data.json"

        with open(input_file, "r") as file:
            content = file.read()
            content = content.replace("Q", "MIKE")
            json_content = json.loads(content)

        output_file = calculate_imputation_factors_method.lambda_handler(
            {"data_json": json_content, "questions_list": "Q601_asphalting_sand,"
                                                          + "Q602_building_soft_sand,"
                                                          + "Q603_concreting_sand,"
                                                          + "Q604_bituminous_gravel,"
                                                          + "Q605_concreting_gravel,"
                                                          + "Q606_other_gravel,"
                                                          + "Q607_constructional_fill"
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
            "tests/fixtures/calculate_imputation_factors_method_input_data.json"
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

                        out = calculate_imputation_factors_wrangler.lambda_handler(
                            mock_event, context_object
                        )
                        assert "success" in out
                        assert not out["success"]
                        assert "Incomplete Lambda response" in out["error"]

    @mock_sns
    @mock_sqs
    def test_wrangler_key_error(self):
        with mock.patch(
            "calculate_imputation_factors_wrangler.aws_functions.get_dataframe"
        ) as mocked:
            mocked.side_effect = KeyError()
            out = calculate_imputation_factors_wrangler.lambda_handler(
                mock_event, context_object
            )
            assert "success" in out
            assert not out["success"]
            assert "Key Error" in out["error"]

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
                out = calculate_imputation_factors_wrangler.lambda_handler(
                    mock_event, context_object
                )
                assert "success" in out
                assert not out["success"]
                assert "AWS Error" in out["error"]

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
            "tests/fixtures/calculate_imputation_factors_method_input_data.json"
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

                    out = calculate_imputation_factors_wrangler.lambda_handler(
                        mock_event, context_object
                    )
                    assert "success" in out
                    assert not out["success"]
                    assert "error message" in out["error"]
