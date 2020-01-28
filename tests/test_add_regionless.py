import json
import unittest
import unittest.mock as mock

import pandas as pd
from botocore.response import StreamingBody
from es_aws_functions import exception_classes
from moto import mock_lambda, mock_s3, mock_sqs
from pandas.util.testing import assert_frame_equal

import add_regionless_method
import add_regionless_wrangler


class MockContext:
    aws_request_id = 666


mock_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "queue_url": "Earl",
    "factors_parameters": {
        "RuntimeVariables": {
            "factors_type": "factors_calcualtion_a",
            "percentage_movement": True,
            "survey_column": "survey",
            "region_column": "region",
            "regionless_code": 14,
            "first_imputation_factor": 1,
            "second_imputation_factor": 2,
            "first_threshold": 7,
            "second_threshold": 7,
            "third_threshold": 9,
            "regional_mean": "third_imputation_factor"
        }
    },
    "run_id": "bob"
  }
}

context_object = MockContext()


class TestApplyFactors(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            'os.environ',
            {
                'sqs_queue_url': '213456',
                'sns_topic_arn': 'mock_arn',
                'checkpoint': '0',
                'method_name': 'mock_method_name',
                'sqs_message_group_id': 'mock_sqs_message_name',
                'bucket_name': 'mock_bucket',
                'incoming_message_group': 'mock_group',
                "in_file_name": "Test",
                "out_file_name": "Test",
            }
        )
        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock_sqs
    @mock_lambda
    @mock_s3
    @mock.patch("add_regionless_wrangler.aws_functions.send_sns_message")
    @mock.patch("add_regionless_wrangler.aws_functions.save_data")
    def test_wrangler_happy_path(self, mock_me, mock_you):
        with mock.patch("add_regionless_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("add_regionless_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/add_regionless_input.json", "r") as file:
                    mock_client_object.invoke.return_value\
                            .get.return_value.read\
                            .return_value.decode.return_value = json.dumps({
                                "data": json.loads(file.read()), "success": True
                            })
                    with open("tests/fixtures/add_regionless_output.json", "rb")\
                            as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        response = add_regionless_wrangler.lambda_handler(
                            mock_event,
                            context_object,
                        )
                        assert "success" in response
                        assert response["success"] is True

    def test_method_happy_path(self):
        input_file = "tests/fixtures/add_regionless_input.json"
        with open(input_file, "r") as file:
            json_dataframe = pd.read_json(file.read())
            json_content = json.loads(json_dataframe.to_json(orient="records"))
            event = {
                "json_data": json_content,
                "regionless_code": 14,
                "region_column": "region"
            }

            output = add_regionless_method.lambda_handler(
                event,
                context_object
            )

            response_df = (pd.read_json(output["data"]))
            expected_df = (pd.read_json('tests/fixtures/add_regionless_output.json'))

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    @mock.patch("add_regionless_wrangler.boto3")
    @mock.patch("add_regionless_wrangler.aws_functions.get_dataframe")
    def test_wrangler_general_exception(self, mock_boto, mock_squeues):
        with mock.patch("add_regionless_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = Exception()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                add_regionless_wrangler.lambda_handler(
                    mock_event,
                    context_object
                )
            assert "General Error" in exc_info.exception.error_message

    def test_method_general_exception(self):
        input_file = "tests/fixtures/add_regionless_input.json"
        with open(input_file, "r") as file:
            json_content = file.read()
            with mock.patch("add_regionless_method.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("General exception")
                event = {
                    "json_data": json_content,
                    "regionless_code": 14,
                    "region_column": "region"
                }

                response = add_regionless_method.lambda_handler(
                    event,
                    context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    @mock_sqs
    @mock_lambda
    @mock.patch("add_regionless_wrangler.aws_functions.get_dataframe")
    def test_wrangler_key_error(self, mock_squeues):
        with mock.patch("add_regionless_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = KeyError()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                add_regionless_wrangler.lambda_handler(
                        mock_event,
                        context_object,
                    )
            assert "Key Error" in exc_info.exception.error_message

    def test_method_key_error(self):
        with open("tests/fixtures/add_regionless_input.json", "r") as file:
            content = file.read()
            event = {
                    "jason_data": content,
                     }
            response = add_regionless_method.lambda_handler(
                event, context_object
            )

            assert """Key Error in""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the method name to allow for test of missing parameter
        add_regionless_wrangler.os.environ.pop("method_name")
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            add_regionless_wrangler.lambda_handler(mock_event, context_object)
        add_regionless_wrangler.os.environ["method_name"] = "mock_method"
        assert "Error validating environment params" in exc_info.exception.error_message

    @mock_sqs
    def test_wrangler_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            add_regionless_wrangler.os.environ,
            {
                "sqs_queue_url": "An Invalid Queue"
            },
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                add_regionless_wrangler.lambda_handler(
                    mock_event, context_object
                )
            assert "AWS Error" in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch("add_regionless_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("add_regionless_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody("{'boo':'moo':}", 2)
                }
                with open("tests/fixtures/add_regionless_input.json", "rb") as queue_file:
                    msgbody = queue_file.read()
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    with unittest.TestCase.assertRaises(
                            self, exception_classes.LambdaFailure) as exc_info:
                        add_regionless_wrangler.lambda_handler(
                            mock_event,
                            context_object,
                        )
                    assert "Bad data" in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("add_regionless_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("add_regionless_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/add_regionless_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 123456)
                    }
                    with open("tests/fixtures/add_regionless_input.json", "rb")\
                            as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        with unittest.TestCase.assertRaises(
                                self, exception_classes.LambdaFailure) as exc_info:
                            add_regionless_wrangler.lambda_handler(
                                mock_event,
                                context_object,
                            )
                        assert "Incomplete Lambda response" in \
                               exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    @mock_s3
    @mock.patch("add_regionless_wrangler.aws_functions.send_sns_message")
    @mock.patch("add_regionless_wrangler.aws_functions.save_data")
    def test_wrangler_method_fail(self, mock_me, mock_you):
        with mock.patch("add_regionless_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("add_regionless_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = \
                    json.dumps({"success": False,
                                "error": "This is an error message"})
                with open("tests/fixtures/add_regionless_input.json", "rb") as queue_file:
                    msgbody = queue_file.read()
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    with unittest.TestCase.assertRaises(
                            self, exception_classes.LambdaFailure) as exc_info:
                        add_regionless_wrangler.lambda_handler(
                            mock_event,
                            context_object,
                        )
                    assert "error message" in exc_info.exception.error_message
