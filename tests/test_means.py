import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from es_aws_functions import exception_classes
from moto import mock_lambda, mock_s3, mock_sqs
from pandas.util.testing import assert_frame_equal

import calculate_means_method
import calculate_means_wrangler


class MockContext:
    aws_request_id = 666


with open("tests/fixtures/means_input.json", "r") as file:
    in_file = file.read()

mock_event = {
    "json_data": json.loads(in_file),
    "distinct_values": ["strata", "region"],
    "questions_list": ['Q601_asphalting_sand',
                       'Q602_building_soft_sand',
                       'Q603_concreting_sand',
                       'Q604_bituminous_gravel',
                       'Q605_concreting_gravel',
                       'Q606_other_gravel',
                       'Q607_constructional_fill']
}

mock_wrangles_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "movement_type": "movement_calculation_b",
    "period": 201809,
    "run_id": "example",
    "distinct_values": ["region"],
    "queue_url": "Earl",
    "questions_list": ['Q601_asphalting_sand',
                       'Q602_building_soft_sand',
                       'Q603_concreting_sand',
                       'Q604_bituminous_gravel',
                       'Q605_concreting_gravel',
                       'Q606_other_gravel',
                       'Q607_constructional_fill'],
    "in_file_name": "calculate_movement_out.json",
    "incoming_message_group_id": "imputation-calculate-movement-out",
    "outgoing_message_group_id": "mock_message",
    "out_file_name": "Test",
  }
}

context_object = MockContext


class TestMeans(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "error_handler_arn": "mock_arn",
                "checkpoint": "mock_checkpoint",
                "method_name": "mock_method",
                "sqs_queue_url": "mock_queue",
                "movement_columns": "movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill",  # noqa: E501
                "current_period": "mock_period",
                "previous_period": "mock_prev_period",
                "run_environment": "development",
                "sns_topic_arn": "mock_arn",
                "bucket_name": "Mike"

            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        # Stop the mocking of the os stuff
        cls.mock_os_patcher.stop()

    @mock_sqs
    @mock_lambda
    @mock_s3
    def test_wrangler_happy_path(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="Mike")
        with mock.patch("calculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/means_input.json", "r") as file:
                    mock_client_object.invoke.return_value\
                        .get.return_value.read\
                        .return_value.decode.return_value = json.dumps({
                            "data": file.read(), "success": True
                        })
                    with open("tests/fixtures/means_input.json", "rb") as queue_file:
                        msgbody = queue_file.read().decode("UTF-8")
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666

                        response = calculate_means_wrangler.lambda_handler(
                            mock_wrangles_event,
                            context_object,
                        )

                        assert "success" in response
                        assert response["success"] is True

    def test_method_happy_path(self):
        mean_col = "mean_Q601_asphalting_sand,mean_Q602_building_soft_sand,mean_Q603_concreting_sand,mean_Q604_bituminous_gravel,mean_Q605_concreting_gravel,mean_Q606_other_gravel,mean_Q607_constructional_fill"  # noqa: E501
        sorting_cols = ["responder_id", "region", "strata"]
        selected_cols = mean_col.split(",")

        output = calculate_means_method.lambda_handler(
            mock_event,
            context_object
        )

        expected_df = (
            pd.read_csv("tests/fixtures/means_output.csv")
            .sort_values(sorting_cols)
            .reset_index()[selected_cols]
        )

        response_df = (
            pd.read_json(output["data"])
            .sort_values(sorting_cols)
            .reset_index()[selected_cols]
        )

        response_df = response_df.round(5)
        expected_df = expected_df.round(5)

        assert_frame_equal(response_df, expected_df)

    def test_wrangler_general_exception(self):
        with mock.patch("calculate_means_wrangler.InputSchema") as mock_schema:
            mock_schema.side_effect = Exception()
            mock_schema_object = mock.Mock()
            mock_schema.return_value = mock_schema_object
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_means_wrangler.lambda_handler(
                    mock_wrangles_event,
                    context_object
                )

            assert "General Error" in exc_info.exception.error_message

    def test_method_general_exception(self):
        with mock.patch("calculate_means_method.pd.DataFrame") as mocked:
            mocked.side_effect = Exception("General exception")
            response = calculate_means_method.lambda_handler(
                mock_event,
                context_object
            )

            assert "success" in response
            assert response["success"] is False
            assert """General exception""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangler_key_error(self):
        with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = KeyError()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_means_wrangler.lambda_handler(
                     mock_event,
                     context_object,
                )
            assert "Key Error" in exc_info.exception.error_message

    def test_method_key_error(self):
        # pass none value to trigger key index error
        response = calculate_means_method.lambda_handler({"mike": "mike"}, context_object)
        assert """Key Error""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        calculate_means_wrangler.os.environ.pop("method_name")
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            calculate_means_wrangler.lambda_handler(mock_wrangles_event, context_object)
        calculate_means_wrangler.os.environ["method_name"] = "mock_method"
        assert "Error validating environment params" in exc_info.exception.error_message

    @mock_sqs
    def test_wrangler_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            calculate_means_wrangler.os.environ,
            {
                "sqs_queue_url": "An Invalid Queue"
            },
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_means_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )
            assert "AWS Error" in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch("calculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody("{'boo':'moo':}", 2)
                }
                with open("tests/fixtures/means_input.json", "rb") as queue_file:
                    msgbody = queue_file.read().decode('UTF-8')
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    with unittest.TestCase.assertRaises(
                            self, exception_classes.LambdaFailure) as exc_info:
                        calculate_means_wrangler.lambda_handler(
                            mock_wrangles_event,
                            context_object,
                        )
                    assert "Bad data" in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("calculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/means_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 123456)
                    }
                    with open("tests/fixtures/means_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        with unittest.TestCase.assertRaises(
                                self, exception_classes.LambdaFailure) as exc_info:
                            calculate_means_wrangler.lambda_handler(
                                mock_wrangles_event,
                                context_object,
                            )
                        assert "Incomplete Lambda response" \
                               in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    @mock_s3
    def test_wrangler_method_fail(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="Mike")
        with mock.patch("calculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value.get.return_value.\
                    read.return_value.decode.return_value \
                    = json.dumps({"success": False,
                                  "error": "This is an error message"})
                with open("tests/fixtures/means_input.json", "rb") as queue_file:
                    msgbody = queue_file.read().decode("UTF-8")
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    with unittest.TestCase.assertRaises(
                            self, exception_classes.LambdaFailure) as exc_info:
                        calculate_means_wrangler.lambda_handler(
                            mock_wrangles_event,
                            context_object,
                        )
                    assert "error message" in exc_info.exception.error_message
