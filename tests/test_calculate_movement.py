import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from moto import mock_lambda, mock_s3, mock_sns, mock_sqs

import calculate_movement_method
import calculate_movement_wrangler


class TestStringMethods(unittest.TestCase):

    @mock_s3
    def test_get__and_save_data_from_to_s3(self):
        # Tests both the save and get from s3 by putting a file in using one method
        # and retrieving it with another
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="MIKE")
        with open("tests/fixtures/wrangler_input_test_data.json", "rb") as file:
            calculate_movement_wrangler.save_to_s3("MIKE", "123", file)

        response = calculate_movement_wrangler.read_data_from_s3("MIKE", "123")
        assert response

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

            response = calculate_movement_method.lambda_handler(input_data,
                                                                {"aws_request_id": "666"})

        assert response == striped_string

    @mock_sns
    def test_publish_sns(self):

        sns = boto3.client('sns', region_name='eu-west-2')
        created = sns.create_topic(Name="some-topic")
        topic_arn = created['TopicArn']

        out = calculate_movement_wrangler.send_sns_message("Imputation was run example!",
                                                           pd.DataFrame(), topic_arn, "3")

        assert (out['ResponseMetadata']['HTTPStatusCode'] == 200)

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
                    {"RuntimeVariables": {"period": 201809}}, {"aws_request_id": "666"})

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
                    {"RuntimeVariables": {"period": 201809}}, {"aws_request_id": "666"})

                assert 'success' in response
                assert response['success'] is False

    @mock_sqs
    @mock_s3
    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.
        :return: None.
        """
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
            }
        ):
            # Removing the previous_period to allow for test of missing parameter
            calculate_movement_method.os.environ.pop("previous_period")

            response = calculate_movement_method.lambda_handler({"RuntimeVariables":
                                                                {"period": "201809"}},
                                                                {"aws_request_id": "666"})

            assert (response['error'].__contains__("""Parameter validation error"""))

    @mock_sqs
    @mock_s3
    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
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
            }
        ):
            # Removing the previous_period to allow for test of missing parameter
            calculate_movement_wrangler.os.environ.pop("checkpoint")

            response = calculate_movement_wrangler.lambda_handler({
                "RuntimeVariables": {"period": "201809"}}, {"aws_request_id": "666"}
            )

            assert (response['error'].__contains__("""Parameter validation error"""))

    @mock_sqs
    def test_no_data_in_queue(self):
        sqs = boto3.client("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_url(QueueName="test_queue")['QueueUrl']

        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
                    'arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
                    's3_file': 'file_to_get_from_s3.json',
                    'bucket_name': 'some-bucket-name',
                    'queue_url': queue_url,
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
            }
        ):
            with mock.patch("calculate_movement_wrangler.read_data_from_s3") as mock_s3:

                with open("tests/fixtures/movements_output.json", "r") as file:
                    mock_content = file.read()

                mock_s3.return_value = mock_content

                response = calculate_movement_wrangler.lambda_handler(
                    {"RuntimeVariables": {"period": 201809}},
                    {"aws_request_id": "666"}
                )

            assert "success" in response
            assert response["success"] is False
            assert (response['error'].__contains__("""There was no data in sqs queue"""))

    @mock_sqs
    def test_fail_to_get_from_sqs(self):
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
            },
        ):
            response = calculate_movement_wrangler.lambda_handler(
                {"RuntimeVariables": {"period": 201809}}, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    def test_method_key_error_exception(self):
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
            }
        ):
            input_file = "tests/fixtures/method_input_test_data.json"

            with open(input_file, "r") as file:
                content = file.read()
                content = content.replace("Q", "TEST")
                json_content = json.loads(content)

            output_file = calculate_movement_method.lambda_handler(
                json_content, {"aws_request_id": "666"}
            )

            assert not output_file["success"]
            assert "Key Error" in output_file["error"]
