import json
import unittest
import unittest.mock as mock

import pandas as pd
from botocore.response import StreamingBody

import calculate_movement_wrangler


class MockContext:
    aws_request_id = 666


with open("tests/fixtures/wrangler_input_test_data.json", "r") as file:
    in_file = file.read()
mock_event = {
    "json_data": json.loads(in_file),
    "distinct_values": ["strata", "region"],
    "questions_list": 'Q601_asphalting_sand,'
                      'Q602_building_soft_sand,'
                      'Q603_concreting_sand,'
                      'Q604_bituminous_gravel,'
                      'Q605_concreting_gravel,'
                      'Q606_other_gravel,'
                      'Q607_constructional_fill'
}

mock_wrangles_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "calculation_type": "movement_calculation_b",
    "period": 201809,
    "id": "example",
    "distinct_values": "strata"
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
        })
        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_os_patcher.stop()

    @mock.patch('calculate_movement_wrangler.funk.send_sns_message_with_anomalies')
    @mock.patch('calculate_movement_wrangler.funk.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.strata_mismatch_detector')
    @mock.patch('calculate_movement_wrangler.funk.save_to_s3')
    @mock.patch('calculate_movement_wrangler.funk.get_dataframe')
    @mock.patch('calculate_movement_wrangler.funk.read_dataframe_from_s3')
    def test_wrangler(self, mock_s3_return, mock_sqs_return, mock_s3_save, mock_strata,
                      mock_lambda, mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/method_output_compare_result.json') as file:
            method_output = json.dumps(json.load(file))

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        with open('tests/fixtures/merged_data.json') as file:
            merged_data = json.load(file)

        mock_s3_return.return_value = pd.DataFrame(previous_data)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        mock_strata.return_value = pd.DataFrame(merged_data), pd.DataFrame()

        myvar = mock_send_sqs.call_args_list

        with open('tests/fixtures/method_output_compare_result.json', "rb") as file:
            mock_lambda.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 13123)}

            response = calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )

        output = myvar[0][0][2]

        assert response['success']
        assert output == method_output

    def test_strata_mismatch_detector(self):
        with open('tests/fixtures/merged_data_no_missmatch.json') as file:
            input_data = json.load(file)

        time = "period"
        reference = "responder_id"
        segmentation = "strata"
        stored_segmentation = "goodstrata"
        current_time = "current_period"
        previous_time = "previous_period"
        current_segmentation = "current_strata"
        previous_segmentation = "previous_strata"

        (response1, response2) = calculate_movement_wrangler.strata_mismatch_detector(
            pd.DataFrame(input_data), 201809, time, reference, segmentation,
            stored_segmentation, current_time, previous_time, current_segmentation,
            previous_segmentation)

        assert response2.shape[0] <= 0

    @mock.patch('calculate_movement_wrangler.funk.send_sns_message_with_anomalies')
    @mock.patch('calculate_movement_wrangler.funk.save_data')
    @mock.patch('calculate_movement_wrangler.funk.get_dataframe')
    @mock.patch('calculate_movement_wrangler.funk.read_dataframe_from_s3')
    def test_full_response(self, mock_s3_return, mock_sqs_return,
                           mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data_full_response.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = previous_data

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        response = calculate_movement_wrangler.lambda_handler(
              mock_wrangles_event, context_object
        )

        self.assertTrue(response["success"])
        self.assertFalse(response["impute"])

    @mock.patch('calculate_movement_wrangler.funk.send_sns_message')
    @mock.patch('calculate_movement_wrangler.funk.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.strata_mismatch_detector')
    @mock.patch('calculate_movement_wrangler.funk.save_to_s3')
    @mock.patch('calculate_movement_wrangler.funk.get_dataframe')
    @mock.patch('calculate_movement_wrangler.funk.read_dataframe_from_s3')
    def test_wrangler_incomplete_json(self, mock_s3_return, mock_sqs_return, mock_s3_save,
                                      mock_strata, mock_lambda, mock_send_sqs,
                                      mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        with open('tests/fixtures/merged_data.json') as file:
            merged_data = json.load(file)

        mock_s3_return.return_value = pd.DataFrame(previous_data)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        mock_strata.return_value = pd.DataFrame(merged_data), pd.DataFrame()

        with open('tests/fixtures/method_output_compare_result.json', "rb") as file:
            mock_lambda.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 2)}

            response = calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )

        assert "success" in response
        assert response["success"] is False
        assert response["error"].__contains__("""Incomplete Lambda response""")

    @mock.patch('calculate_movement_wrangler.funk.send_sns_message')
    @mock.patch('calculate_movement_wrangler.funk.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.strata_mismatch_detector')
    @mock.patch('calculate_movement_wrangler.funk.save_to_s3')
    @mock.patch('calculate_movement_wrangler.funk.get_dataframe')
    @mock.patch('calculate_movement_wrangler.funk.read_dataframe_from_s3')
    def testing_wrangler_bad_data(self, mock_s3_return, mock_sqs_return,
                                  mock_s3_save, mock_strata, mock_lambda,
                                  mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        with open('tests/fixtures/merged_data.json') as file:
            merged_data = json.load(file)

        mock_s3_return.return_value = previous_data

        mock_sqs_return.return_value = json.dumps(input_data), 666

        mock_strata.return_value = pd.DataFrame(merged_data), pd.DataFrame()

        with open('tests/fixtures/method_output_compare_result.json', "rb") as file:
            mock_lambda.return_value.invoke.return_value = {
                "Payload": StreamingBody("{'boo':'moo'}", 2)
            }

            response = calculate_movement_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )

        assert "success" in response
        assert response["success"] is False
        assert response["error"].__contains__("""Bad data encountered""")

    @mock.patch('calculate_movement_wrangler.funk.send_sns_message')
    @mock.patch('calculate_movement_wrangler.funk.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.strata_mismatch_detector')
    @mock.patch('calculate_movement_wrangler.funk.save_to_s3')
    @mock.patch('calculate_movement_wrangler.funk.get_dataframe')
    @mock.patch('calculate_movement_wrangler.funk.read_dataframe_from_s3')
    def test_wrangler_key_error_exception(self, mock_s3_return, mock_sqs_return,
                                          mock_s3_save, mock_strata, mock_lambda,
                                          mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = previous_data
        mock_sqs_return.side_effect = KeyError("sdfg")
        mock_sqs_return.return_value = json.dumps(input_data), 666

        response = calculate_movement_wrangler.lambda_handler(
            mock_wrangles_event, context_object
        )

        assert "success" in response
        assert not response["success"]
        assert "Key Error" in response["error"]

    @mock.patch('calculate_movement_wrangler.funk.send_sns_message_with_anomalies')
    @mock.patch('calculate_movement_wrangler.funk.save_data')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.strata_mismatch_detector')
    @mock.patch('calculate_movement_wrangler.funk.save_to_s3')
    @mock.patch('calculate_movement_wrangler.funk.get_dataframe')
    @mock.patch('calculate_movement_wrangler.funk.read_dataframe_from_s3')
    def test_wrangler_method_fail(self, mock_s3_return, mock_sqs_return,
                                  mock_s3_save, mock_strata, mock_lambda,
                                  mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        with open('tests/fixtures/merged_data.json') as file:
            merged_data = json.load(file)

        mock_s3_return.return_value = pd.DataFrame(previous_data)

        mock_sqs_return.return_value = pd.DataFrame(input_data), 666

        mock_strata.return_value = pd.DataFrame(merged_data), pd.DataFrame()

        mock_lambda.return_value.invoke.return_value.get.return_value \
            .read.return_value.decode.return_value = \
            json.dumps({"ADictThatWillTriggerError": "someValue",
                        "error": "This is an error message"})
        response = calculate_movement_wrangler.lambda_handler(
            mock_wrangles_event, context_object
        )

        assert not response["success"]
        assert "error message" in response["error"]
