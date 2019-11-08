import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_s3, mock_sqs

import recalculate_means_wrangler


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
                'queue_url': 'mock_queue',
                'questions_list': 'Q601_asphalting_sand Q602_building_soft_sand '
                                  + 'Q603_concreting_sand Q604_bituminous_gravel '
                                  + 'Q605_concreting_gravel Q606_other_gravel '
                                  + 'Q607_constructional_fill',
                'sqs_messageid_name': 'mock_message',
                'arn': 'mock_arn',
                "incoming_message_group": "I am GROOP",
                "in_file_name": "Test",
                "out_file_name": "Test",
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
        with mock.patch("recalculate_means_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("recalculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/"
                          "recalculate_means_method_output.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 9066)
                    }
                    with open("tests/fixtures/"
                              "recalculate_means_input.json", "rb") as queue_file:
                        msgbody = queue_file.read().decode("UTF-8")
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666

                        response = recalculate_means_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is True

    def test_wrangler_exception_handling(self):
        """
        testing the exception handler works within the wrangler.

        :param self:
        :return: mock response
        """
        response = recalculate_means_wrangler.lambda_handler(None,
                                                             {"aws_request_id": "666"})
        assert not response['success']

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        """

        :return:
        """
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
                recalculate_means_wrangler.os.environ,
                {
                    "checkpoint": "",
                    "queue_url": queue_url
                }
        ):
            # Removing the checkpoint to allow for test of missing parameter
            recalculate_means_wrangler.os.environ.pop("questions_list")
            response = recalculate_means_wrangler.lambda_handler(
                {"RuntimeVariables":

                 {"checkpoint": 123}}, {"aws_request_id": "666"})
            assert """Error validating environment parameters:""" in response['error']

    @mock_sqs
    def test_client_error(self):
        response = recalculate_means_wrangler.lambda_handler(
            {"RuntimeVariables":
                {"checkpoint": 123}}, {"aws_request_id": "666"})
        assert 'success' in response
        assert not response['success']
        assert "AWS Error" in response['error']

    @mock_sqs
    def test_attribute_error(self):
        with open('tests/fixtures/recalculate_means_input.json') as file:
            input_data = file.read()
        with mock.patch('recalculate_means_wrangler.boto3.client') as mock_boto:
            with mock.patch('recalculate_means_wrangler.funk.get_dataframe') as mock_funk:
                mocked_client = mock.Mock()
                mock_boto.return_value = mocked_client
                mock_funk.return_value = input_data, 666

                mocked_client.invoke.return_value = {"Payload": "mike"}

                response = recalculate_means_wrangler.lambda_handler(None,
                                                                     {"aws_request_id":
                                                                      "666"})
        assert 'success' in response
        assert not response['success']
        assert "Bad data" in response['error']

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
            response = recalculate_means_wrangler.lambda_handler(None,
                                                                 {"aws_request_id":
                                                                  "666"})
        assert 'success' in response
        assert not response['success']
        assert "Key Error" in response['error']

    @mock_sqs
    def test_general_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
                recalculate_means_wrangler.os.environ, {"queue_url": queue_url}
        ):
            with mock.patch("recalculate_means_wrangler.boto3.client") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                response = recalculate_means_wrangler.lambda_handler(
                    "", {"aws_request_id": "666"}
                )
                assert "success" in response
                assert response["success"] is False
                assert "General Error" in response['error']

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("recalculate_means_wrangler.funk.get_dataframe") as mock_squeues:
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

                        response = recalculate_means_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert """Incomplete Lambda response""" in response["error"]