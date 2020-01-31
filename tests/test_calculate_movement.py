import json
import unittest
import unittest.mock as mock

from es_aws_functions import exception_classes
from moto import mock_lambda, mock_s3, mock_sqs

import calculate_movement_method
import calculate_movement_wrangler


class MockContext:
    aws_request_id = 666


with open("tests/fixtures/method_input_test_data.json", "r") as file:
    in_file = file.read()

mock_event = {
            "json_data": in_file,
            "movement_type": "movement_calculation_a",
            "distinct_values": ["region"],
            "questions_list": ["Q601_asphalting_sand",
                               "Q602_building_soft_sand",
                               "Q603_concreting_sand",
                               "Q604_bituminous_gravel",
                               "Q605_concreting_gravel",
                               "Q606_other_gravel",
                               "Q607_constructional_fill"],
            "current_period": 201809,
            "period_column": "period",
            "periodicity": "03",
            "in_file_name": {
                "imputation_movement": "Test"
            },
            "incoming_message_group": {
                "imputation_movement": "bananas"
            }
}

mock_event_b = {
            "json_data": in_file,
            "movement_type": "movement_calculation_b",
            "distinct_values": ["region"],
            "questions_list": ["Q601_asphalting_sand",
                               "Q602_building_soft_sand",
                               "Q603_concreting_sand",
                               "Q604_bituminous_gravel",
                               "Q605_concreting_gravel",
                               "Q606_other_gravel",
                               "Q607_constructional_fill"],
            "current_period": 201809,
            "period_column": "period",
            "periodicity": "03",
            "in_file_name": {
                "imputation_movement": "Test"
            },
            "incoming_message_group": {
                "imputation_movement": "bananas"
            }
}

mock_wrangles_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "movement_type": "movement_calculation_b",
    "period": 201809,
    "period_column": "period",
    "periodicity": "03",
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
    "in_file_name": {
          "imputation_movement": "Test"
    },
    "incoming_message_group": {
        "imputation_movement": "bananas"
    }
  }
}

context_object = MockContext


class TestStringMethods(unittest.TestCase):

    def test_lambda_handler_movement_method(self):
        with open("tests/fixtures/method_output_compare_result.json") as file:
            result = json.load(file)

        string_result = json.dumps(result)
        striped_string = string_result.replace(" ", "")

        response = calculate_movement_method.lambda_handler(mock_event,
                                                            context_object)
        assert json.loads(response["data"]) == json.loads(striped_string)

    def test_lambda_handler_movement_method_b(self):
        with open("tests/fixtures/method_2_output_compare_result.json") as file:
            result = json.load(file)

        string_result = json.dumps(result)
        striped_string = string_result.replace(" ", "")

        response = calculate_movement_method.lambda_handler(mock_event_b,
                                                            context_object)

        assert json.loads(response["data"]) == json.loads(striped_string)

    @mock_sqs
    @mock_lambda
    def test_wrangler_catch_exception(self):
        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:8:some-topic',
            's3_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
            'output_file': 'output_file.json',
            'reference': 'responder_id',
            'current_time': 'current_period',
            'previous_time': 'previous_period',
            'incoming_message_group': 'bananas',
            'in_file_name': 'Test',
            'out_file_name': 'Test',
            'previous_period_file': 'test',
            'period': '202020',
            'non_response_file': 'Test',
        }):

            # using get_from_s3 to force exception early on.

            with mock.patch('calculate_movement_wrangler'
                            '.aws_functions.read_dataframe_from_s3') as mocked:

                mocked.side_effect = Exception('SQS Failure')
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    calculate_movement_wrangler.lambda_handler(
                        mock_wrangles_event, context_object)
                assert "General Error" in exc_info.exception.error_message

    @mock_sqs
    @mock_lambda
    def test_method_catch_exception(self):
        with mock.patch('calculate_movement_method.pd.DataFrame') as mocked:
            mocked.side_effect = Exception('SQS Failure')

            response = calculate_movement_method.lambda_handler(
                mock_event, context_object)

            assert 'success' in response
            assert response['success'] is False

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
            'sqs_message_group_id': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
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
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_movement_wrangler.lambda_handler(mock_wrangles_event,
                                                           context_object)
            assert "Parameter validation error" in exc_info.exception.error_message

    @mock_sqs
    @mock_s3
    def test_fail_to_get_from_sqs(self):
        with mock.patch.dict(calculate_movement_wrangler.os.environ, {
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'previous_period_file': 'file_to_get_from_s3.json',
            'bucket_name': 'some-bucket-name',
            'sqs_message_group_id': 'output_something_something',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'time': 'period',
            'response_type': 'response_type',
            'non_response_file': 'output_file.json',
            'reference': 'responder_id',
            'segmentation': 'strata',
            'current_time': 'current_period',
            'previous_time': 'previous_period',
            'out_file_name': 'Test'
            },
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_movement_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )
            assert "AWS Error" in exc_info.exception.error_message

    def test_method_key_error_exception(self):
        output_file = calculate_movement_method.lambda_handler(
            {"mike": "mike"}, context_object
        )

        assert not output_file["success"]
        assert "Key Error" in output_file["error"]
