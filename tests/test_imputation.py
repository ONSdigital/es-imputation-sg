import json
import os
import sys
import unittest.mock as mock

import boto3
from moto import mock_sns, mock_sqs

import calculate_means_method
import calculate_means_wrangler

sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))


class TestImputation:
    # Sending a message to SQS - Wrangler
    @mock_sqs
    def test_message_send_without_attributes(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        queue = sqs.create_queue(QueueName="blah")
        queue_url = sqs.get_queue_by_name(QueueName="blah").url
        calculate_means_wrangler.send_sqs_message(queue_url, "", "", "")

        messages = queue.receive_messages()
        assert len(messages) == 1

    # Sending a message to SNS - Wrangler
    @mock_sns
    def test_sns_send(self):
        sns = boto3.client('sns', region_name='eu-west-2')
        topic = sns.create_topic(Name='bloo')
        topic_arn = topic['TopicArn']
        calculate_means_wrangler.send_sns_message(topic_arn, "", "")

    # Testing output against expected output - Method
    def test_data_output(self):
        with open('tests/fixtures/movements_output.json') as file:
            input_data = json.load(file)
        with open('tests/fixtures/means_output.json') as file:
            means_data = file.read()

        with mock.patch.dict(calculate_means_method.os.environ, {
              'queue_url': 'queue_url',
              'current_period': '201809',
              'previous_period': '201806',
              'questions_list': 'movement_Q601_asphalting_sand' +
                'movement_Q602_building_soft_sand' +
                'movement_Q603_concreting_sand' +
                'movement_Q604_bituminous_gravel' +
                'movement_Q605_concreting_gravel' +
                'movement_Q606_other_gravel' +
                'movement_Q607_constructional_fill'
          }):

            response = calculate_means_method.lambda_handler(input_data, None)
            assert response == means_data

    # Testing SQS functionality - Method
    def test_catch_exception(self):
        # Method
        with mock.patch('calculate_means_method.boto3') as mocked:
            mocked.client.side_effect = Exception('SQS Failure')
            response = calculate_means_method.lambda_handler("", None)
            assert 'success' in response
            assert response['success'] is False
