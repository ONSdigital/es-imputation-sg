from moto import mock_sns, mock_sqs, mock_s3
import json
import boto3
import pandas as pd
import calculate_movement_method
import calculate_movement_wrangler
import unittest
import unittest.mock as mock


class TestStringMethods(unittest.TestCase):

    def test_lambda_handler_movement_method(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
            'current_period': '201809',
            'previous_period': '201806',
            'questions_list': 'Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand '
                              'Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel '
                              'Q607_constructional_fill'
        }):

            with open('method_input_test_data.json') as file: input_data = json.load(file)
            with open('method_output_compare_result.json') as file: result = json.load(file)

            string_result = json.dumps(result)
            striped_string = string_result.replace(" ", "")

            response = calculate_movement_method.lambda_handler(input_data, None)

        assert response == striped_string

    @mock_sns
    def test_publish_sns(self):
        mock = mock_s3()
        mock.start()
        boto3.setup_default_session()

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        with mock.patch.dict(calculate_movement_method.os.environ, {
            "arn": topic_arn,
            "checkpoint": "3"
        }):

            calculate_movement_wrangler.send_sns_message("Imputation was run example!", pd.DataFrame())

        mock.stop()

    @mock_sqs
    def test_get_sqs(self):
        mock = mock_s3()
        mock.start()
        boto3.setup_default_session()

        sqs_send = boto3.client('sqs')
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        sqs.create_queue(QueueName="test_queue")
        test_message = "Example"

        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        sqs_send.send_message(QueueUrl=queue_url, MessageBody=test_message)

        messages = calculate_movement_wrangler.get_data_from_sqs(queue_url)

        # Response is a list if there is a message in the queue and a dict if no message is present.
        assert messages['Messages'][0]['Body'] == test_message
        mock.stop()

    @mock_sqs
    def test_sqs_messages_send(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        queue = sqs.create_queue(QueueName="test_queue_test.fifo", Attributes={'FifoQueue': 'true'})
        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        calculate_movement_wrangler.send_sqs_message(queue_url, "{}", "test_group_id")

        messages = queue.receive_messages()
        #messages = calculate_movement_wrangler.get_data_from_sqs(queue_url)
        assert type(messages) == list
