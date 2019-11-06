import json
import unittest.mock as mock

import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_sqs
from pandas.util.testing import assert_frame_equal

import atypicals_method
import atypicals_wrangler


class TestClass():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            'os.environ',
            {
                'queue_url': '213456',
                'arn': 'mock_arn',
                'checkpoint': '0',
                'atypical_columns': 'atyp601,atyp602,atyp603,atyp604,atyp605,atyp606,atyp607',  # noqa E501
                'iqrs_columns': 'iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607',
                'movement_columns': 'movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill',  # noqa: E501
                'mean_columns': 'mean601,mean602,mean603,mean604,mean605,mean606,mean607',
                'method_name': 'mock_method_name',
                'sqs_messageid_name': 'mock_sqs_message_name',
                'error_handler_arn': 'mock_error_handler_arn',
                'bucket_name': 'mock_bucket',
                'input_data': 'mock_data',
                'incoming_message_group': 'mock_group',
                "in_file_name": "Test",
                "out_file_name": "Test",
            }
        )
        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock_sqs
    @mock_lambda
    @mock.patch("atypicals_wrangler.funk.send_sns_message")
    @mock.patch("atypicals_wrangler.funk.save_data")
    def test_wrangler_happy_path(self, mock_me, mock_you):
        with mock.patch("atypicals_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("atypicals_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("atypical_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 416503)
                    }
                    with open("atypical_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                        response = atypicals_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )
                        assert "success" in response
                        assert response["success"] is True

    def test_method_happy_path(self):
        input_file = "atypical_input.json"
        with open(input_file, "r") as file:
            movement_col = 'movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill'  # noqa: E501
            sorting_cols = ['responder_id', 'region', 'strata']
            selected_cols = movement_col.split(',')

            json_content = file.read()
            output = atypicals_method.lambda_handler(
                json_content,
                {"aws_request_id": "666"}
            )

            response_df = (
                pd.DataFrame(output)
                .sort_values(sorting_cols)
                .reset_index()[selected_cols]
            )

            expected_df = (
                pd.read_json('atypical_scala_output.json')
                .sort_values(sorting_cols)
                .reset_index()[selected_cols]
            )

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    @mock.patch("atypicals_wrangler.boto3")
    @mock.patch("atypicals_wrangler.funk.get_dataframe")
    def test_wrangler_general_exception(self, mock_boto, mock_squeues):
        with mock.patch("atypicals_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = Exception()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            response = atypicals_wrangler.lambda_handler(
                None,
                {"aws_request_id": "666"}
            )

            assert "success" in response
            assert response["success"] is False
            assert """General Error""" in response["error"]

    def test_method_general_exception(self):
        input_file = "atypical_input.json"
        with open(input_file, "r") as file:
            json_content = file.read()
            with mock.patch("atypicals_method.pd.read_json") as mocked:
                mocked.side_effect = Exception("General exception")
                response = atypicals_method.lambda_handler(
                    json_content,
                    {"aws_request_id": "666"}
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    @mock_sqs
    @mock_lambda
    @mock.patch("atypicals_wrangler.funk.get_dataframe")
    def test_wrangler_key_error(self, mock_squeues):
        with mock.patch("atypicals_wrangler.boto3.client") as mock_client:
            mock_client.side_effect = KeyError()
            mock_client_object = mock.Mock()
            mock_client.return_value = mock_client_object
            response = atypicals_wrangler.lambda_handler(
                    None,
                    {"aws_request_id": "666"},
                )

            assert "success" in response
            assert response["success"] is False
            assert """Key Error""" in response["error"]

    def test_method_key_error(self):
        with mock.patch.dict(
            "os.environ",
            {
                "mean_columns": "bum"
            }
        ):
            with open("atypical_input.json", "r") as file:
                content = file.read()

                response = atypicals_method.lambda_handler(
                    content, {"aws_request_id": "666"}
                )
                assert """Key Error in""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        atypicals_wrangler.os.environ.pop("method_name")
        response = atypicals_wrangler.lambda_handler(None, {"aws_request_id": "666"})
        atypicals_wrangler.os.environ["method_name"] = "mock_method"
        assert """Error validating environment params:""" in response["error"]

    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.
        :return: None.
        """
        input_file = "atypical_input.json"
        with open(input_file, "r") as file:
            json_content = file.read()
            # Removing arn to allow for test of missing parameter
            atypicals_method.os.environ.pop("mean_columns")
            response = atypicals_method.lambda_handler(
                json_content,
                {"aws_request_id": "666"}
            )
            atypicals_method.os.environ["mean_columns"] = "mean601,mean602,mean603,mean604,mean605,mean606,mean607"  # noqa E501
            assert """Error validating environment params:""" in response["error"]

    @mock_sqs
    def test_wrangler_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            atypicals_wrangler.os.environ,
            {
                "queue_url": "An Invalid Queue"
            },
        ):
            response = atypicals_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """AWS Error""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch("atypicals_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("atypicals_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody("{'boo':'moo':}", 2)
                }
                with open("means_input.json", "rb") as queue_file:
                    msgbody = queue_file.read()
                    mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666
                    response = atypicals_wrangler.lambda_handler(
                        None,
                        {"aws_request_id": "666"},
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert """Bad data""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("atypicals_wrangler.funk.get_dataframe") as mock_squeues:
            with mock.patch("atypicals_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("atypical_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 123456)
                    }
                    with open("atypical_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = pd.DataFrame(json.loads(msgbody)), 666

                        response = atypicals_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert """Incomplete Lambda response""" in response["error"]
