import unittest.mock as mock
import atypicals_wrangler, atypicals_method
import os
from botocore.response import StreamingBody
import pandas as pd
import json
import pytest
from pandas.util.testing import assert_frame_equal
import sys
import io


class TestClass():
    @classmethod
    def setup_class(cls):
        cls.mock_boto_wrangler_patcher = mock.patch('atypicals_wrangler.boto3')
        cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()

        cls.mock_boto_method_patcher = mock.patch('atypicals_method.boto3')
        cls.mock_boto_method = cls.mock_boto_method_patcher.start()

        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'queue_url': '213456',
            'arn': 'mock_arn',
            'checkpoint': '0',
            'atypical_columns': 'atyp601,atyp602,atyp603,atyp604,atyp605,atyp606,atyp607',
            'iqrs_columns': 'iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607',
            'movement_columns': 'movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill',
            'mean_columns': 'mean601,mean602,mean603,mean604,mean605,mean606,mean607',
            'method_name': 'mock_method_name',
            'sqs_messageid_name': 'mock_sqs_message_name',
            'error_handler_arn': 'mock_error_handler_arn',
            'bucket_name': 'mock_bucket',
            'input_data': 'mock_data'
        })
        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_boto_method_patcher.stop()
        cls.mock_os_patcher.stop()

    def test_integration(self):
        FILE_NAME_IN = "atypical_input.json"
        MOVEMENT_COL = 'movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill'
        SORTING_COLS = ['responder_id', 'region', 'strata']
        SELECTED_COLS = MOVEMENT_COL.split(',')

        with open(FILE_NAME_IN, "r") as file:
            json_content = json.loads(file.read())

        mocked_client = mock.Mock()
        self.mock_boto_wrangler.client.return_value.invoke = mocked_client

        with mock.patch('json.loads') as mock_json:
            mock_json.return_value = json_content

            atypicals_wrangler.lambda_handler(None, None)

        payload = mocked_client.call_args[1]['Payload']
        response = atypicals_method.lambda_handler(json.loads(payload), None)

        responseDF = pd.DataFrame(response).sort_values(SORTING_COLS).reset_index()[SELECTED_COLS]

        expectedDF = pd.read_json('atypical_scala_output.json').sort_values(SORTING_COLS).reset_index()[SELECTED_COLS]

        responseDF = responseDF.round(5)
        expectedDF = expectedDF.round(5)

        assert_frame_equal(responseDF, expectedDF)

    def test_wrangler_exception_handling(self):
        response = atypicals_wrangler.lambda_handler(None, None)
        assert not response['success']

    def test_method_exception_handling(self):
        json_content ='[{"movement_Q601_asphalting_sand":0.0},{"movement_Q601_asphalting_sand":0.857614899}]'

        response = atypicals_method.lambda_handler(json_content, None)
        assert not response['success']

    def test_wrangler_success_responses(self):
        with mock.patch('atypicals_wrangler.json') as mock_json:
            response = atypicals_wrangler.lambda_handler(None, None)
            assert mock_json.dumps.call_args[0][0]['success']
            assert response