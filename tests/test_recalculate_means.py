import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from es_aws_functions import exception_classes
from moto import mock_lambda, mock_s3, mock_sqs

import recalculate_means_wrangler


class MockContext:
    aws_request_id = 666


with open("tests/fixtures/recalculate_means_input.json", "r") as file:
    in_file = file.read()

mock_event = {
    "json_data": json.loads(in_file),
    "distinct_values": ["strata", "region"],
    "questions_list": ['Q601_asphalting_sand,'
                       'Q602_building_soft_sand,'
                       'Q603_concreting_sand,'
                       'Q604_bituminous_gravel,'
                       'Q605_concreting_gravel,'
                       'Q606_other_gravel,'
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
    "questions_list": ["Q601_asphalting_sand",
                       "Q602_building_soft_sand",
                       "Q603_concreting_sand",
                       "Q604_bituminous_gravel",
                       "Q605_concreting_gravel",
                       "Q606_other_gravel",
                       "Q607_constructional_fill"],
    "in_file_name": "test",
    "incoming_message_group_id": "test",
    "out_file_name": "Test",
    'outgoing_message_group_id': 'mock_message'
  }
}

context_object = MockContext


class TestRecalculateMeans(unittest.TestCase):
    """
    Test Class Recalculate Means Wrangler.
    """
    @classmethod
    def setup_class(cls):
        """
        sets up the mock boto clients and starts the patchers.
        :return: None.
        """
        # setting up the mock environment variables for the wrangler
        cls.mock_os_wrangler_patcher = mock.patch.dict(
            'os.environ', {
                'checkpoint': 'mock_checkpoint',
                'error_handler_arn': 'mock_arn',
                'method_name': 'mock_method',
                'sns_topic_arn': 'mock_arn',
                "run_environment": "development",
                "bucket_name": "Mike"
            }
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()

    @classmethod
    def teardown_class(cls):
        """
        stops the wrangler, method and os patchers.

        :return: None.
        """
        cls.mock_os_wrangler_patcher.stop()

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
        with mock.patch("recalculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("recalculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/"
                          "recalculate_means_method_output.json", "r") as file:
                    mock_client_object.invoke.return_value\
                        .get.return_value.read\
                        .return_value.decode.return_value = json.dumps({
                            "data": file.read(), "success": True
                        })
                    with open("tests/fixtures/"
                              "recalculate_means_input.json", "rb") as queue_file:
                        msgbody = queue_file.read().decode("UTF-8")
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666

                        response = recalculate_means_wrangler.lambda_handler(
                            mock_wrangles_event,
                            context_object,
                        )

                        assert "success" in response
                        assert response["success"] is True

    def test_wrangler_exception_handling(self):
        """
        testing the exception handler works within the wrangler.

        :param self:
        :return: mock response
        """
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            recalculate_means_wrangler.lambda_handler(mock_wrangles_event,
                                                      context_object)
        assert "AWS Error" in exc_info.exception.error_message

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        """

        :return:
        """
        with mock.patch.dict(
                recalculate_means_wrangler.os.environ,
                {
                    "checkpoint": ""
                }
        ):
            # Removing the checkpoint to allow for test of missing parameter
            recalculate_means_wrangler.os.environ.pop("checkpoint")
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                recalculate_means_wrangler.lambda_handler(
                    mock_wrangles_event, context_object)
            assert "Error validating environment parameters" \
                   in exc_info.exception.error_message

    @mock_sqs
    def test_client_error(self):
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            recalculate_means_wrangler.lambda_handler(
                mock_wrangles_event, context_object)
        assert "AWS Error" in exc_info.exception.error_message

    @mock_sqs
    def test_attribute_error(self):
        with open('tests/fixtures/recalculate_means_input.json') as file:
            input_data = file.read()
        with mock.patch('recalculate_means_wrangler.boto3.client') as mock_boto:
            with mock.patch('recalculate_means_wrangler.aws_functions.get_dataframe')\
                    as mock_funk:
                mocked_client = mock.Mock()
                mock_boto.return_value = mocked_client
                mock_funk.return_value = input_data, 666

                mocked_client.invoke.return_value = {"Payload": "mike"}
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    recalculate_means_wrangler.lambda_handler(mock_wrangles_event,
                                                              context_object)
                assert "Bad data" in exc_info.exception.error_message

    @mock_sqs
    def test_key_error(self):
        with open('tests/fixtures/recalculate_means_input.json') as file:
            input_data = file.read()
        with mock.patch('recalculate_means_wrangler.boto3.client') as mock_boto:
            mocked_client = mock.Mock()
            mock_boto.return_value = mocked_client
            mocked_client.receive_message.return_value = {
                                "Messages": [{"F#": input_data, "ReceiptHandle": 666}]
                            }
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                recalculate_means_wrangler.lambda_handler(mock_wrangles_event,
                                                          context_object)

        assert "Key Error" in exc_info.exception.error_message

    @mock_sqs
    def test_general_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
                recalculate_means_wrangler.os.environ, {"sqs_queue_url": sqs_queue_url}
        ):
            with mock.patch("recalculate_means_wrangler.boto3.client") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    recalculate_means_wrangler.lambda_handler(
                        "", context_object
                    )
                assert "General Error in" in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("recalculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("recalculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/"
                          "recalculate_means_method_output.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 123456)
                    }
                    with open("tests/fixtures/"
                              "recalculate_means_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        with unittest.TestCase.assertRaises(
                                self, exception_classes.LambdaFailure) as exc_info:
                            recalculate_means_wrangler.lambda_handler(
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
        with mock.patch("recalculate_means_wrangler.aws_functions.get_dataframe")\
                as mock_squeues:
            with mock.patch("recalculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value.get.return_value \
                    .read.return_value.decode.return_value = \
                    json.dumps({"success": False,
                                "error": "This is an error message"})
                with open("tests/fixtures/"
                          "recalculate_means_input.json", "rb") as queue_file:
                    msgbody = queue_file.read().decode("UTF-8")
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    with unittest.TestCase.assertRaises(
                            self, exception_classes.LambdaFailure) as exc_info:
                        recalculate_means_wrangler.lambda_handler(
                            mock_wrangles_event,
                            context_object,
                        )
                    assert "error message" in exc_info.exception.error_message
