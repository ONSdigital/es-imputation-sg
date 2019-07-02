from moto import mock_sns, mock_sqs
import json
import boto3
import pandas as pd
import sys
import os
import unittest
import unittest.mock as mock
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))
import calculate_movement_method
import calculate_movement_wrangler


class TestStringMethods(unittest.TestCase):

    def test_lambda_handler_movement_method(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
           'current_period': '201809',
           'previous_period': '201806',
           'questions_list': 'Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand '
                             'Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel '
                             'Q607_constructional_fill'
        }):

            with open("tests/method_input_test_data.json") as file: input_data = json.load(file)
            with open("tests/method_output_compare_result.json") as file: result = json.load(file)

            string_result = json.dumps(result)
            striped_string = string_result.replace(" ", "")

            response = calculate_movement_method.lambda_handler(input_data, None)

        assert response == striped_string

    @mock_sns
    def test_publish_sns(self):

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        with mock.patch.dict(calculate_movement_method.os.environ, {
            "arn": topic_arn,
            "Checkpoint": "3"
        }):

            calculate_movement_wrangler.send_sns_message("Imputation was run example!", pd.DataFrame())

    @mock_sqs
    def test_sqs_messages(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        sqs.create_queue(QueueName="test_queue_test.fifo", Attributes={'FifoQueue': 'true'})
        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        calculate_movement_wrangler.send_sqs_message(queue_url, "{'Test': 'Message'}",
                                                     "test_group_id", "hello123")

        messages = calculate_movement_wrangler.get_data_from_sqs(queue_url)

        # Response is a list if there is a message in the queue and a dict if no message is present.
        assert messages['Messages'][0]['Body'] == "{'Test': 'Message'}"
