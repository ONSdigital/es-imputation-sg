from moto import mock_sns, mock_sqs, mock_lambda
import calculate_movement_method
import calculate_movement_wrangler
import json
import boto3
import pandas as pd
import unittest
import unittest.mock as mock


class TestStringMethods(unittest.TestCase):

    def test_lambda_handler_movement_method(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
           'current_period': '201809',
           'previous_period': '201806',
           'questions_list': 'Q601_asphalting_sand '
                             'Q602_building_soft_sand '
                             'Q603_concreting_sand '
                             'Q604_bituminous_gravel '
                             'Q605_concreting_gravel '
                             'Q606_other_gravel '
                             'Q607_constructional_fill'
        }):

            with open("tests/fixtures/method_input_test_data.json") as file:
                input_data = json.load(file)
            with open("tests/fixtures/method_output_compare_result.json") as file:
                result = json.load(file)

            string_result = json.dumps(result)
            striped_string = string_result.replace(" ", "")

            response = calculate_movement_method.lambda_handler(input_data, None)

        assert response == striped_string

    @mock_sns
    def test_publish_sns(self):

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        calculate_movement_wrangler.send_sns_message("Imputation was run example!",
                                                     pd.DataFrame(), topic_arn, "3")

    @mock_sqs
    def test_sqs_messages(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')

        sqs.create_queue(QueueName="test_queue_test.fifo",
                         Attributes={'FifoQueue': 'true'})

        queue_url = sqs.get_queue_by_name(QueueName="test_queue_test.fifo").url

        calculate_movement_wrangler.send_sqs_message(queue_url, "{'Test': 'Message'}",
                                                     "test_group_id", "hello123")

        messages = calculate_movement_wrangler.get_data_from_sqs(queue_url)

        # Response is a list if there is a message in the
        # queue and a dict if no message is present.
        assert messages['Messages'][0]['Body'] == "{'Test': 'Message'}"

    @mock_sqs
    @mock_lambda
    def test_wrangler_catch_exception(self):
        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
            'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_messageid_name': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
            'questions_list': 'Q601_asphalting_sand '
                              'Q602_building_soft_sand '
                              'Q603_concreting_sand '
                              'Q604_bituminous_gravel '
                              'Q605_concreting_gravel '
                              'Q606_other_gravel '
                              'Q607_constructional_fill',
            'output_file': 'output_file.json',
            'reference': 'responder_id',
            'segmentation': 'strata',
            'stored_segmentation': 'goodstrata',
            'current_time': 'current_period',
            'previous_time': 'previous_period',
            'current_segmentation': 'current_strata',
            'previous_segmentation': 'previous_strata'
        }):

            # using get_from_s3 to force exception early on.

            with mock.patch('calculate_movement_wrangler.read_data_from_s3') as mocked:

                mocked.side_effect = Exception('SQS Failure')

                response = calculate_movement_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}}, None)

                assert 'success' in response
                assert response['success'] is False

    @mock_sqs
    @mock_lambda
    def test_method_catch_exception(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
            'current_period': '201809',
            'previous_period': '201806',
            'questions_list': 'Q601_asphalting_sand '
                              'Q602_building_soft_sand '
                              'Q603_concreting_sand '
                              'Q604_bituminous_gravel '
                              'Q605_concreting_gravel '
                              'Q606_other_gravel '
                              'Q607_constructional_fill'
        }):

            with mock.patch('calculate_movement_method.pd.DataFrame') as mocked:
                mocked.side_effect = Exception('SQS Failure')

                response = calculate_movement_method.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}}, None)

                assert 'success' in response
                assert response['success'] is False
