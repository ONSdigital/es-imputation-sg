"""
Tests for Recalculate Means Wrangler.
"""
import unittest
from unittest import mock
import json
import os
import sys
import pandas as pd
import boto3
from moto import mock_sns, mock_sqs
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
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
                'arn': 'mock_arn'
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

    @mock.patch('recalculate_means_wrangler.boto3.client')
    def test_wrangler(self, mock_lambda):
        """
        mocks functionality of the wrangler:
        - load json file
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.
        """
        with open('tests/fixtures/recalculate_means_input.json') as file:
            input_data = json.load(file)
        with open('tests/fixtures/recalculate_means_method_output.json', "rb") as file:
            method_output = json.load(file)

        with mock.patch('json.loads')as json_loads:
            json_loads.return_value = input_data

            recalculate_means_wrangler.lambda_handler(None, None)

        payload = mock_lambda.return_value.invoke.call_args[1]['Payload']

        # check the output file contains the expected columns and non null values
        payload_dataframe = pd.DataFrame(method_output)
        required_columns = {
            'mean_Q601_asphalting_sand',
            'mean_Q602_building_soft_sand',
            'mean_Q603_concreting_sand',
            'mean_Q604_bituminous_gravel',
            'mean_Q605_concreting_gravel',
            'mean_Q606_other_gravel',
            'mean_Q607_constructional_fill'
        }

        self.assertTrue(required_columns.issubset(set(payload_dataframe.columns)),
                        'Recalculate Means Columns not in the DataFrame.')
        new_columns = payload_dataframe[required_columns]

        self.assertFalse(new_columns.isnull().values.any())

    def test_wrangler_exception_handling(self):
        """
        testing the exception handler works within the wrangler.

        :param self:
        :return: mock response
        """
        response = recalculate_means_wrangler.lambda_handler(None, None)
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
                     {"checkpoint": 123}}, None)
            # self.assertRaises(ValueError)
            assert(response['error'].__contains__(
                """ValueError: Error validating environment parameters:"""))

    @mock_sns
    def test_sns_messages(self):
        """
        Test sending sns messages to the queue.

        :return: None.
        """
        with mock.patch.dict(recalculate_means_wrangler.os.environ, {"arn": "test_arn"}):
            sns = boto3.client("sns", region_name="eu-west-2")
            topic = sns.create_topic(Name="test_topic")
            topic_arn = topic["TopicArn"]
            recalculate_means_wrangler.send_sns_message(topic_arn,
                                                        "test_runtype",
                                                        "test_checkpoint")

    @mock_sqs
    def test_sqs_send_message(self):
        """
        Tests sending of sqs messages to the queue.

        :return: None.
        """
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        sqs.create_queue(QueueName="test_queue_test.fifo",
                         Attributes={'FifoQueue': 'true'})
        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        recalculate_means_wrangler.send_sqs_message(queue_url,
                                                    "{'Test': 'Message'}",
                                                    "test_group_id")
        messages = recalculate_means_wrangler.get_sqs_message(queue_url)
        assert messages['Messages'][0]['Body'] == "{'Test': 'Message'}"
