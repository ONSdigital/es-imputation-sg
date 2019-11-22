import json
import unittest
import unittest.mock as mock

from moto import mock_lambda, mock_s3, mock_sqs

import calculate_movement_method
import calculate_movement_wrangler


class MockContext:
    aws_request_id = 666


with open("tests/fixtures/method_input_test_data.json", "r") as file:
    in_file = file.read()
mock_event = {
            "json_data": in_file,
            "calculation_type": "movement_calculation_a",
            "distinct_values": "region"
        }

mock_wrangles_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "calculation_type": "movement_calculation_b",
    "period": 201809,
    "id": "example",
    "distinct_values": "region"
  }
}

context_object = MockContext


class TestStringMethods(unittest.TestCase):

    def test_lambda_handler_movement_method(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
           'current_period': '201809',
           'previous_period': '201806',
           'questions_list': 'Q601_asphalting_sand,'
                             'Q602_building_soft_sand,'
                             'Q603_concreting_sand,'
                             'Q604_bituminous_gravel,'
                             'Q605_concreting_gravel,'
                             'Q606_other_gravel,'
                             'Q607_constructional_fill'
        }):

            with open("tests/fixtures/method_output_compare_result.json") as file:
                result = json.load(file)

            string_result = json.dumps(result)
            striped_string = string_result.replace(" ", "")

            response = calculate_movement_method.lambda_handler(mock_event,
                                                                context_object)

        assert response == striped_string

    @mock_sqs
    @mock_lambda
    def test_wrangler_catch_exception(self):
        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:8:some-topic',
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
            'questions_list': 'Q601_asphalting_sand,'
                              'Q602_building_soft_sand,'
                              'Q603_concreting_sand,'
                              'Q604_bituminous_gravel,'
                              'Q605_concreting_gravel,'
                              'Q606_other_gravel,'
                              'Q607_constructional_fill',
            'output_file': 'output_file.json',
            'reference': 'responder_id',
            'segmentation': 'strata',
            'stored_segmentation': 'goodstrata',
            'current_time': 'current_period',
            'previous_time': 'previous_period',
            'current_segmentation': 'current_strata',
            'previous_segmentation': 'previous_strata',
            'incoming_message_group': 'bananas',
            'in_file_name': 'Test',
            'out_file_name': 'Test',
        }):

            # using get_from_s3 to force exception early on.

            with mock.patch('calculate_movement_wrangler'
                            '.funk.read_dataframe_from_s3') as mocked:

                mocked.side_effect = Exception('SQS Failure')

                response = calculate_movement_wrangler.lambda_handler(
                    mock_wrangles_event, context_object)

                assert 'success' in response
                assert response['success'] is False

    @mock_sqs
    @mock_lambda
    def test_method_catch_exception(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
            'current_period': '201809',
            'previous_period': '201806',
            'questions_list': 'Q601_asphalting_sand,'
                              'Q602_building_soft_sand,'
                              'Q603_concreting_sand,'
                              'Q604_bituminous_gravel,'
                              'Q605_concreting_gravel,'
                              'Q606_other_gravel,'
                              'Q607_constructional_fill'
        }):

            with mock.patch('calculate_movement_method.pd.DataFrame') as mocked:
                mocked.side_effect = Exception('SQS Failure')

                response = calculate_movement_method.lambda_handler(
                    mock_event, context_object)

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
            'questions_list': 'Q601_asphalting_sand,'
                              'Q602_building_soft_sand,'
                              'Q603_concreting_sand,'
                              'Q604_bituminous_gravel,'
                              'Q605_concreting_gravel,'
                              'Q606_other_gravel,'
                              'Q607_constructional_fill'
            }
        ):
            # Removing the previous_period to allow for test of missing parameter
            calculate_movement_method.os.environ.pop("previous_period")

            response = calculate_movement_method.lambda_handler(mock_event,
                                                                context_object)

            assert (response['error'].__contains__("""Parameter validation error"""))

    @mock_sqs
    @mock_s3
    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                         '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
            'questions_list': 'Q601_asphalting_sand,'
                              'Q602_building_soft_sand,'
                              'Q603_concreting_sand,'
                              'Q604_bituminous_gravel,'
                              'Q605_concreting_gravel,'
                              'Q606_other_gravel,'
                              'Q607_constructional_fill',
            'output_file': 'output_file.json',
            'reference': 'responder_id',
            'segmentation': 'strata',
            'stored_segmentation': 'goodstrata',
            'current_time': 'current_period',
            'previous_time': 'previous_period',
            'current_segmentation': 'current_strata',
            'previous_segmentation': 'previous_strata',
            'incoming_message_group': 'bananas',
            'in_file_name': 'Test',
            'out_file_name': 'Test',
            }
        ):
            # Removing the previous_period to allow for test of missing parameter
            calculate_movement_wrangler.os.environ.pop("checkpoint")

            response = calculate_movement_wrangler.lambda_handler(mock_wrangles_event,
                                                                  context_object)

            assert (response['error'].__contains__("""Parameter validation error"""))

    @mock_sqs
    @mock_s3
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'previous_period_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_queue_url': 'https://sqs.eu-west-2.amazonaws.com/'
                                    '82618934671237/SomethingURL.fifo',
            'sqs_message_group_id': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
            'questions_list': 'Q601_asphalting_sand,'
                              'Q602_building_soft_sand,'
                              'Q603_concreting_sand,'
                              'Q604_bituminous_gravel,'
                              'Q605_concreting_gravel,'
                              'Q606_other_gravel,'
                              'Q607_constructional_fill',
            'non_response_file': 'output_file.json',
            'reference': 'responder_id',
            'segmentation': 'strata',
            'stored_segmentation': 'goodstrata',
            'current_time': 'current_period',
            'previous_time': 'previous_period',
            'current_segmentation': 'current_strata',
            'previous_segmentation': 'previous_strata',
            'incoming_message_group': 'bananas',
            'in_file_name': 'Test',
            'out_file_name': 'Test',
            },
        ):
            response = calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )
            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    def test_method_key_error_exception(self):
        with mock.patch.dict(calculate_movement_method.os.environ, {
            'current_period': '201809',
            'previous_period': '201806',
            'questions_list': 'Q601_asphalting_sand,'
                              'Q602_building_soft_sand,'
                              'Q603_concreting_sand,'
                              'Q604_bituminous_gravel,'
                              'Q605_concreting_gravel,'
                              'Q606_other_gravel,'
                              'Q607_constructional_fill'
            }
        ):

            output_file = calculate_movement_method.lambda_handler(
                {"mike": "mike"}, context_object
            )

            assert not output_file["success"]
            assert "Key Error" in output_file["error"]
