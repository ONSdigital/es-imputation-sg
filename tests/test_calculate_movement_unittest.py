import calculate_movement_wrangler
import calculate_movement_method
import unittest.mock as mock
import unittest
import pandas as pd
import json
from botocore.response import StreamingBody


class TestClass(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.mock_boto_wrangler_patcher = mock.patch('calculate_movement_wrangler.boto3')
        cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()

        cls.mock_os_patcher = mock.patch.dict('os.environ', {
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
        })
        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_os_patcher.stop()

    @mock.patch('calculate_movement_wrangler.send_sns_message')
    @mock.patch('calculate_movement_wrangler.send_sqs_message')
    @mock.patch('calculate_movement_wrangler.boto3.client')
    @mock.patch('calculate_movement_wrangler.strata_mismatch_detector')
    @mock.patch('calculate_movement_wrangler.save_to_s3')
    @mock.patch('calculate_movement_wrangler.get_data_from_sqs')
    @mock.patch('calculate_movement_wrangler.read_data_from_s3')
    def test_wrangler(self, mock_s3_return, mock_sqs_return, mock_s3_save, mock_strata,
                      mock_lambda, mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/method_output_compare_result.json') as file:
            method_output = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        with open('tests/fixtures/merged_data.json') as file:
            merged_data = json.load(file)

        mock_s3_return.return_value = previous_data

        mock_sqs_return.return_value = {"Messages": [{"Body": json.dumps(input_data),
                                                     "ReceiptHandle": "String"}]}

        mock_strata.return_value = pd.DataFrame(merged_data), pd.DataFrame()

        myvar = mock_send_sqs.call_args_list

        with open('tests/fixtures/method_output_compare_result.json', "rb") as file:
            mock_lambda.return_value.invoke.return_value = {"Payload":
                                                            StreamingBody(file, 13123)}

            response = calculate_movement_wrangler.lambda_handler(
                {"RuntimeVariables": {"period": "201809"}}, None
            )

        output = myvar[0][0][1]

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

    def test_wrangler_traceback(self):

        traceback = calculate_movement_wrangler._get_traceback(Exception('Test'))

        assert traceback == 'Exception: Test\n'

    def test_method_traceback(self):
        traceback = calculate_movement_method._get_traceback(Exception('Test'))

        assert traceback == 'Exception: Test\n'

    @mock.patch('calculate_movement_wrangler.send_sns_message')
    @mock.patch('calculate_movement_wrangler.send_sqs_message')
    @mock.patch('calculate_movement_wrangler.get_data_from_sqs')
    @mock.patch('calculate_movement_wrangler.read_data_from_s3')
    def test_full_response(self, mock_s3_return, mock_sqs_return,
                           mock_send_sqs, mock_sns_message):

        with open('tests/fixtures/wrangler_input_test_data_full_response.json') as file:
            input_data = json.load(file)

        with open('tests/fixtures/s3_previous_period_data.json') as file:
            previous_data = json.load(file)

        mock_s3_return.return_value = previous_data

        mock_sqs_return.return_value = {"Messages": [{"Body": json.dumps(input_data),
                                                      "ReceiptHandle": "String"}]}

        response = calculate_movement_wrangler.lambda_handler(
              {"RuntimeVariables": {"period": "201809"}}, None
        )

        mock_send_sqs.call_args_list

        self.assertTrue(response["success"])
        self.assertFalse(response["Impute"])
