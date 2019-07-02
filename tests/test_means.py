from unittest import mock
import os
import unittest
import ipdb
import json
import pandas as pd
import calculate_means_wrangler
import calculate_means_method
from pandas.util.testing import assert_frame_equal


class test_means(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'checkpoint': '3',
            'error_handler_arn': 'mock_arn',
            'sqs_messageid_name': 'mock_message',
            'checkpoint': 'mock_checkpoint',
            'function_name': 'mock_method',
            'queue_url': 'mock_queue',
            'questions_list': 'Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel Q607_constructional_fill',
            'movement_columns': 'movement_Q601_asphalting_sand movement_Q602_building_soft_sand movement_Q603_concreting_sand movement_Q604_bituminous_gravel movement_Q605_concreting_gravel movement_Q606_other_gravel movement_Q607_constructional_fill region strata',
            'current_period': 'mock_period',
            'previous_period': 'mock_prev_period',
            'arn': 'mock_arn'
            })

        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        # Stop the mocking of the os stuff
        cls.mock_os_patcher.stop()

    @mock.patch('calculate_means_wrangler.boto3')
    def test_wrangler(self,mock_boto):
        #patch boto3 environ on second_mean_method
        with open('means_input.json','r') as file:
            json_content = json.loads(file.read())
        with mock.patch('json.loads')as json_loads:
            json_loads.return_value = json_content
            calculate_means_wrangler.lambda_handler(None,None)
        payload = mock_boto.client.return_value.invoke.call_args[1]['Payload']

        with open("mean_input_with_columns.json","w+") as file:
            file.write(payload)

        payloadDF = pd.read_json(payload,orient='records')


        required_cols = set(os.environ['questions_list'].split(' '))

        self.assertTrue(required_cols.issubset(set(payloadDF.columns)),'Means columns are not in the DataFrame')

        new_cols = payloadDF[required_cols]
        self.assertFalse(new_cols.isnull().values.any())

    @mock.patch('calculate_means_method.boto3')
    def test_method(self,mock_boto):
        MEAN_COL = 'mean_Q601_asphalting_sand,mean_Q602_building_soft_sand,mean_Q603_concreting_sand,mean_Q604_bituminous_gravel,mean_Q605_concreting_gravel,mean_Q606_other_gravel,mean_Q607_constructional_fill'
        SORTING_COLS = ['responder_id', 'region', 'strata']
        SELECTED_COLS = MEAN_COL.split(',')
        input_file = 'mean_input_with_columns.json'

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())
        output = calculate_means_method.lambda_handler(json_content, None)

        expectedDF = pd.read_csv('means_output.csv').sort_values(SORTING_COLS).reset_index()[SELECTED_COLS]


        responseDF = pd.read_json(output).sort_values(SORTING_COLS).reset_index()[SELECTED_COLS]

        responseDF = responseDF.round(5)
        expectedDF = expectedDF.round(5)

        assert_frame_equal(responseDF, expectedDF)

    @mock.patch('calculate_means_wrangler.boto3')
    def test_wrangler_exception_handling(self,mock_boto):
        response = calculate_means_wrangler.lambda_handler(None, None)
        assert not response['success']

    @mock.patch('calculate_means_method.boto3')
    def test_method_exception_handling(self,mock_boto):
        json_content ='[{"movement_Q601_asphalting_sand":0.0},{"movement_Q601_asphalting_sand":0.857614899}]'

        response = calculate_means_method.lambda_handler(json_content, None)
        assert not response['success']

    @mock.patch('calculate_means_wrangler.boto3')
    def test_wrangler_success_responses(self,mock_boto):
        with mock.patch('calculate_means_wrangler.json') as mock_json:
            response = calculate_means_wrangler.lambda_handler(None, None)
            assert mock_json.dumps.call_args[0][0]['success']
            assert response



if __name__ == '__main__':
    unittest.main