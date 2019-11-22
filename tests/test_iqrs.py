import json
import unittest.mock as mock

import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_s3, mock_sqs
from pandas.util.testing import assert_frame_equal

import iqrs_method
import iqrs_wrangler


class MockContext:
    aws_request_id = 666


mock_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "calculation_type": "movement_calculation_b",
    "period": 201809,
    "id": "example",
    "distinct_values": "region"
  }
}

context_object = MockContext


class TestWranglerAndMethod():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'sqs_queue_url': 'mock_queue',
            'bucket_name': 'mock_bucket',
            'incoming_message_group': 'mock_group',
            'in_file_name': 'Test',
            'out_file_name': 'Test',
            'sqs_message_group_id': 'mock_message',
            'sns_topic_arn': 'mock_arn',
            'checkpoint': 'mock_checkpoint',
            'method_name': 'mock_method',
            'input_data': 'mock_data',
            'error_handler_arn': 'mock_arn',
            'iqrs_columns': 'iqrs_Q601_asphalting_sand,' +
                            'iqrs_Q602_building_soft_sand,' +
                            'iqrs_Q603_concreting_sand,' +
                            'iqrs_Q604_bituminous_gravel,' +
                            'iqrs_Q605_concreting_gravel,' +
                            'iqrs_Q606_other_gravel,' +
                            'iqrs_Q607_constructional_fill',
            'movement_columns': 'movement_Q601_asphalting_sand,' +
                                'movement_Q602_building_soft_sand,' +
                                'movement_Q603_concreting_sand,' +
                                'movement_Q604_bituminous_gravel,' +
                                'movement_Q605_concreting_gravel,' +
                                'movement_Q606_other_gravel,' +
                                'movement_Q607_constructional_fill',
            'distinct_values': 'region, strata'
            })

        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock_sqs
    @mock_s3
    @mock_lambda
    @mock.patch("iqrs_wrangler.funk.send_sns_message")
    @mock.patch("iqrs_wrangler.funk.save_data")
    def test_wrangler_happy_path(self, mock_me, mock_you):
        with mock.patch("iqrs_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/iqrs_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 228382)
                    }
                    with open("tests/fixtures/iqrs_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        response = iqrs_wrangler.lambda_handler(
                            mock_event,
                            context_object,
                        )
                        assert "success" in response
                        assert response["success"] is True

    def test_method_happy_path(self):
        input_file = "tests/fixtures/Iqrs_with_columns.json"
        with open(input_file, "r") as file:
            iqrs_cols = ('iqrs_Q601_asphalting_sand,' +
                         'iqrs_Q602_building_soft_sand,' +
                         'iqrs_Q603_concreting_sand,' +
                         'iqrs_Q604_bituminous_gravel,' +
                         'iqrs_Q605_concreting_gravel,' +
                         'iqrs_Q606_other_gravel,' +
                         'iqrs_Q607_constructional_fill')

            sorting_cols = ['region', 'strata']
            selected_cols = iqrs_cols.split(',') + sorting_cols

            json_content = {
                "data": json.loads(file.read()),
                "distinct_values": "region,strata"
            }

            output = iqrs_method.lambda_handler(json_content, context_object)

            response_df = pd.DataFrame(output).sort_values(sorting_cols)\
                .reset_index()[selected_cols].drop_duplicates(keep='first')\
                .reset_index(drop=True)

            expected_df = pd.read_csv("tests/fixtures/iqrs_scala_output.csv")\
                .sort_values(sorting_cols).reset_index()[selected_cols]

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    @mock.patch("iqrs_wrangler.boto3")
    @mock.patch("iqrs_wrangler.funk.get_dataframe")
    def test_wrangler_general_exception(self, mock_boto, mock_squeues):
        with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = Exception()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            response = iqrs_wrangler.lambda_handler(
                mock_event,
                context_object
            )

            assert "success" in response
            assert response["success"] is False
            assert """General Error""" in response["error"]

    def test_method_general_exception(self):
        input_file = "tests/fixtures/Iqrs_with_columns.json"
        with open(input_file, "r") as file:
            json_content = {
                "data": json.loads(file.read()),
                "distinct_values": ['region', 'strata']
            }
            with mock.patch("iqrs_method.pd.read_json") as mocked:
                mocked.side_effect = Exception("General exception")
                response = iqrs_method.lambda_handler(
                    json_content,
                    context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    @mock_sqs
    @mock_lambda
    @mock.patch("iqrs_wrangler.funk.get_dataframe")
    def test_wrangler_key_error(self, mock_squeues):
        with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = KeyError()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            response = iqrs_wrangler.lambda_handler(
                    mock_event,
                    context_object,
                )

            assert "success" in response
            assert response["success"] is False
            assert """Key Error""" in response["error"]

    def test_method_key_error(self):
        with mock.patch.dict(
            "os.environ",
            {
                "iqrs_columns": "bum"
            }
        ):
            with open("tests/fixtures/Iqrs_with_columns.json", "r") as file:
                json_content = {
                    "data": json.loads(file.read()),
                    "distinct_values": "'region', 'strata'"
                }

                response = iqrs_method.lambda_handler(
                    json_content, context_object
                )
                assert """Key Error in""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        iqrs_wrangler.os.environ.pop("method_name")
        response = iqrs_wrangler.lambda_handler(mock_event, context_object)
        iqrs_wrangler.os.environ["method_name"] = "mock_method"
        assert """Error validating environment params:""" in response["error"]

    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.
        :return: None.
        """
        input_file = "tests/fixtures/Iqrs_with_columns.json"
        with open(input_file, "r") as file:
            json_content = file.read()
            # Removing sns_topic_arn to allow for test of missing parameter
            iqrs_method.os.environ.pop("iqrs_columns")
            response = iqrs_method.lambda_handler(json_content, context_object)
            iqrs_method.os.environ["iqrs_columns"] = (
                    'iqrs_Q601_asphalting_sand,' +
                    'iqrs_Q602_building_soft_sand,' +
                    'iqrs_Q603_concreting_sand,' +
                    'iqrs_Q604_bituminous_gravel,' +
                    'iqrs_Q605_concreting_gravel,' +
                    'iqrs_Q606_other_gravel,' +
                    'iqrs_Q607_constructional_fill')
            assert """Error validating environment params:""" in response["error"]

    @mock_sqs
    def test_wrangler_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            iqrs_wrangler.os.environ,
            {
                "sqs_queue_url": "An Invalid Queue"
            },
        ):
            response = iqrs_wrangler.lambda_handler(
                mock_event, context_object
            )
            assert "success" in response
            assert response["success"] is False
            assert """AWS Error""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch("iqrs_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody("{'boo':'moo':}", 2)
                }
                with open("tests/fixtures/iqrs_input.json", "rb") as queue_file:
                    msgbody = queue_file.read()
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    response = iqrs_wrangler.lambda_handler(
                        mock_event,
                        context_object,
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert """Bad data""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("iqrs_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("tests/fixtures/iqrs_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 123456)
                    }
                    with open("tests/fixtures/iqrs_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        response = iqrs_wrangler.lambda_handler(
                            mock_event,
                            context_object,
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert """Incomplete Lambda response""" in response["error"]
