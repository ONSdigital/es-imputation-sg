"""
Tests for Recalculate Means Wrangler.
"""
import unittest
from unittest import mock
import json
import pandas as pd
import recalculate_means_wrangler


class Test_Recalculate_Means(unittest.TestCase):
    """
    Test Class Recalculate Means Wrangler.
    """
    @classmethod
    def setup_class(cls):
        """
        sets up the mock boto clients and starts the patchers.
        :return: None.
        """
        # setting up the mock environment variables for the wrangler
        cls.mock_os_wrangler_patcher = mock.patch.dict(
            'os.environ', {
                'checkpoint': 'mock_checkpoint',
                'error_handler_arn': 'mock_arn',
                'function_name': 'mock_method',
                'queue_url': 'mock_queue',
                'questions_list': 'Q601_asphalting_sand Q602_building_soft_sand '
                                  + 'Q603_concreting_sand Q604_bituminous_gravel '
                                  + 'Q605_concreting_gravel Q606_other_gravel '
                                  + 'Q607_constructional_fill',
                'sqs_messageid_name': 'mock_message',
                'arn': 'mock_arn'
            }
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()

    @classmethod
    def teardown_class(cls):
        """
        stops the wrangler, method and os patchers.
        :return: None.
        """
        cls.mock_os_wrangler_patcher.stop()


    @mock.patch('recalculate_means_wrangler.boto3.client')
    def test_wrangler(self, mock_lambda):
        """
        mocks functionality of the wrangler:
        - load json file
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.
        """
        with open('tests/recalculate_means_input.json') as file:
            input_data = json.load(file)
        with open('tests/recalculate_means_method_output.json', "rb") as file:
            method_output = json.load(file)

        with mock.patch('json.loads')as json_loads:
            json_loads.return_value = input_data

            recalculate_means_wrangler.lambda_handler(None, None)

        payload = mock_lambda.return_value.invoke.call_args[1]['Payload']

        # check the output file contains the expected columns and non null values
        payload_dataframe = pd.DataFrame(method_output)
        required_columns = {
            'mean_Q601_asphalting_sand',
            'mean_Q602_building_soft_sand',
            'mean_Q603_concreting_sand',
            'mean_Q604_bituminous_gravel',
            'mean_Q605_concreting_gravel',
            'mean_Q606_other_gravel',
            'mean_Q607_constructional_fill'
        }

        self.assertTrue(required_columns.issubset(set(payload_dataframe.columns)),
                        'Recalculate Means Columns not in the DataFrame.')
        new_columns = payload_dataframe[required_columns]

        self.assertFalse(new_columns.isnull().values.any())

    def test_wrangler_exception_handling(self):
        """
        testing the exception handler works within the wrangler.
        :param self:
        :return: mock response
        """
        response = recalculate_means_wrangler.lambda_handler(None, None)
        assert not response['success']
