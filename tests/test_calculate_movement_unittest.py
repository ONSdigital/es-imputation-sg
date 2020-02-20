import json
import unittest
import unittest.mock as mock

import pandas as pd
from botocore.response import StreamingBody
from es_aws_functions import exception_classes

import calculate_movement_wrangler


class MockContext:
    aws_request_id = 666


with open("tests/fixtures/wrangler_input_test_data.json", "r") as file:
    in_file = file.read()

mock_event = {
    "json_data": json.loads(in_file),
    "distinct_values": ["strata", "region"],
    "periodicity": "03",
    "questions_list": ["Q601_asphalting_sand",
                       "Q602_building_soft_sand",
                       "Q603_concreting_sand",
                       "Q604_bituminous_gravel",
                       "Q605_concreting_gravel",
                       "Q606_other_gravel",
                       "Q607_constructional_fill"]
}

mock_wrangles_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "movement_type": "movement_calculation_b",
    "period": 201809,
    "period_column": "period",
    "periodicity": "03",
    "run_id": "example",
    "distinct_values": ["strata"],
    "queue_url": "Earl",
    "questions_list": ["Q601_asphalting_sand",
                       "Q602_building_soft_sand",
                       "Q603_concreting_sand",
                       "Q604_bituminous_gravel",
                       "Q605_concreting_gravel",
                       "Q606_other_gravel",
                       "Q607_constructional_fill"],
    "in_file_name": "Test",
    "out_file_name_skip": "not_apply_out.json",
    "incoming_message_group_id": "bananas",
    'out_file_name': 'Test',
    'outgoing_message_group_id': 'output_something_something',
    'outgoing_message_group_id_skip': 'output_something_something',
    'previous_data': 'file_to_get_from_s3.json',
    'current_data': ''
  }
}

context_object = MockContext


class TestClass(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.mock_boto_wrangler_patcher = mock.patch('calculate_movement_wrangler.boto3')
        cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()

        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'sns_topic_arn': 'arn:aws:sns:eu-west-2:014669633018:some-topic',
            'bucket_name': 'some-bucket-name',
            'checkpoint': '3',
            'method_name': 'method_name_here',
            'response_type': 'response_type',
            'reference': 'responder_id',
            'stored_segmentation': 'goodstrata',
            'current_time': 'current_period',
            'previous_time': 'previous_period'
        })
        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_os_patcher.stop()

    @mock.patch('calculate_movement_wrangler.' +
                'aws_functions.send_sns_message')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3')
    @mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe')
    @mock.patch('calculate_movement_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler(self, mock_s3_return, mock_sqs_return, mock_client,
                      mock_lambda, mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/method_output_compare_result.json') as file:
            method_output = json.dumps(json.load(file))

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = pd.DataFrame(previous_data)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        myvar = mock_send_sqs.call_args_list

        with open('tests/fixtures/method_output_compare_result.json', "r") as file:
            mock_lambda.return_value.invoke.return_value.get.return_value \
                .read.return_value.decode.return_value = \
                json.dumps({"success": True,
                            "data": file.read()})

            response = calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )

        output = myvar[0][0][2]

        assert response['success']
        assert output == method_output

    @mock.patch('calculate_movement_wrangler.aws_functions.send_sns_message')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_data')
    @mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3')
    def test_full_response(self, mock_s3, mock_sqs_return,
                           mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data_full_response.json') as file:
            input_data = json.load(file)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        response = calculate_movement_wrangler.lambda_handler(
              mock_wrangles_event, context_object
        )

        self.assertTrue(response["success"])
        self.assertFalse(response["impute"])

    @mock.patch('calculate_movement_wrangler.aws_functions.send_sns_message')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3')
    @mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe')
    @mock.patch('calculate_movement_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_incomplete_json(self, mock_s3_return, mock_sqs_return, mock_s3_save,
                                      mock_lambda, mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = pd.DataFrame(previous_data)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        with open('tests/fixtures/method_output_compare_result.json', "rb") as file:
            mock_lambda.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 2)}
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_movement_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )
            assert "Incomplete Lambda response" in exc_info.exception.error_message

    @mock.patch('calculate_movement_wrangler.aws_functions.send_sns_message')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3')
    @mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe')
    @mock.patch('calculate_movement_wrangler.aws_functions.read_dataframe_from_s3')
    def testing_wrangler_bad_data(self, mock_s3_return, mock_sqs_return,
                                  mock_s3_save, mock_lambda,
                                  mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = pd.DataFrame(json.load(file))

        mock_sqs_return.return_value = input_data, 666

        with open('tests/fixtures/method_output_compare_result.json', "rb") as file:
            mock_lambda.return_value.invoke.return_value = {
                "Payload": StreamingBody("{'boo':'moo'}", 2)
            }
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                calculate_movement_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )
        assert "Bad data" in exc_info.exception.error_message

    @mock.patch('calculate_movement_wrangler.aws_functions.send_sns_message')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3')
    @mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe')
    @mock.patch('calculate_movement_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_key_error_exception(self, mock_s3_return, mock_sqs_return,
                                          mock_s3_save, mock_lambda,
                                          mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = previous_data
        mock_sqs_return.side_effect = KeyError("sdfg")
        mock_sqs_return.return_value = json.dumps(input_data), 666
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )
        assert "Key Error" in exc_info.exception.error_message

    @mock.patch('calculate_movement_wrangler.' +
                'aws_functions.send_sns_message')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.aws_functions.save_to_s3')
    @mock.patch('calculate_movement_wrangler.aws_functions.get_dataframe')
    @mock.patch('calculate_movement_wrangler.aws_functions.read_dataframe_from_s3')
    def test_wrangler_method_fail(self, mock_s3_return, mock_sqs_return,
                                  mock_s3_save, mock_lambda,
                                  mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = pd.DataFrame(previous_data)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        mock_lambda.return_value.invoke.return_value.get.return_value \
            .read.return_value.decode.return_value = \
            json.dumps({"success": False,
                        "error": "This is an error message"})
        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )
        assert "error message" in exc_info.exception.error_message
