import json
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_sqs
from pandas.util.testing import assert_frame_equal

import iqrs_method
import iqrs_wrangler


class TestWranglerAndMethod():
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict('os.environ', {
            'queue_url': 'mock_queue',
            'sqs_messageid_name': 'mock_message',
            'arn': 'mock_arn',
            'checkpoint': 'mock_checkpoint',
            'method_name': 'mock_method',
            'input_data': 'mock_data',
            'error_handler_arn': 'mock_arn',
            'iqrs_columns': 'iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607',
            'movement_columns': 'movement_Q601_asphalting_sand,movement_Q602_building_soft_sand,movement_Q603_concreting_sand,movement_Q604_bituminous_gravel,movement_Q605_concreting_gravel,movement_Q606_other_gravel,movement_Q607_constructional_fill'  # noqa: E501
            })

        cls.mock_os = cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock_sqs
    @mock_lambda
    def test_wrangler_happy_path(self):
        with mock.patch("iqrs_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("iqrs_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 228382)
                    }
                    with open("iqrs_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = {
                            "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                        }
                        response = iqrs_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is True

    def test_method_happy_path(self):
        input_file = "Iqrs_with_columns.json"
        with open(input_file, "r") as file:
            iqrs_cols = 'iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607'
            sorting_cols = ['region', 'strata']
            selected_cols = iqrs_cols.split(',') + sorting_cols

            json_content = json.loads(file.read())
            output = iqrs_method.lambda_handler(json_content, {"aws_request_id": "666"})

            response_df = pd.DataFrame(output).sort_values(sorting_cols).reset_index()[selected_cols].drop_duplicates(keep='first').reset_index(drop=True)  # noqa: E501

            expected_df = pd.read_csv("iqrs_scala_output.csv").sort_values(sorting_cols).reset_index()[selected_cols]  # noqa: E501

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    @mock.patch("iqrs_wrangler.boto3")
    def test_wrangler_general_exception(self, mock_boto):
        with mock.patch("iqrs_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("iqrs_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 416503)
                    }
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = {
                        "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                    }
                    with mock.patch("iqrs_wrangler.pd.DataFrame") as mocked:
                        mocked.side_effect = Exception("General exception")
                        response = iqrs_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"}
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert """General exception""" in response["error"]

    def test_method_general_exception(self):
        input_file = "Iqrs_with_columns.json"
        with open(input_file, "r") as file:
            json_content = file.read()
            with mock.patch("iqrs_method.pd.read_json") as mocked:
                mocked.side_effect = Exception("General exception")
                response = iqrs_method.lambda_handler(
                    json_content,
                    {"aws_request_id": "666"}
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangler_key_error(self):
        with mock.patch("iqrs_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("iqrs_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 416503)
                    }
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = {
                        "Messages": [{"Sausages": msgbody, "ReceiptHandle": 666}]
                    }
                    response = iqrs_wrangler.lambda_handler(
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
                "iqrs_columns": "bum"
            }
        ):
            with open("Iqrs_with_columns.json", "r") as file:
                content = json.loads(file.read())

                response = iqrs_method.lambda_handler(
                    content, {"aws_request_id": "666"}
                )
                assert """Key Error in""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        iqrs_wrangler.os.environ.pop("method_name")
        response = iqrs_wrangler.lambda_handler(None, {"aws_request_id": "666"})
        iqrs_wrangler.os.environ["method_name"] = "mock_method"
        assert """Error validating environment params:""" in response["error"]

    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.
        :return: None.
        """
        input_file = "Iqrs_with_columns.json"
        with open(input_file, "r") as file:
            json_content = file.read()
            # Removing arn to allow for test of missing parameter
            iqrs_method.os.environ.pop("iqrs_columns")
            response = iqrs_method.lambda_handler(json_content, {"aws_request_id": "666"})
            iqrs_method.os.environ["iqrs_columns"] = "iqrs601,iqrs602,iqrs603,iqrs604,iqrs605,iqrs606,iqrs607"  # noqa E501
            assert """Error validating environment params:""" in response["error"]

    @mock_sqs
    def test_no_data_in_queue(self):
        sqs = boto3.client("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_url(QueueName="test_queue")["QueueUrl"]
        with mock.patch.dict(
            iqrs_wrangler.os.environ,
            {
                "queue_url": queue_url
            },
        ):
            response = iqrs_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """There was no data in sqs queue in""" in response["error"]

    @mock_sqs
    def test_wrangler_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            iqrs_wrangler.os.environ,
            {
                "queue_url": "An Invalid Queue"
            },
        ):
            response = iqrs_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """AWS Error""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch("iqrs_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody("{'boo':'moo':}", 2)
                }
                with open("iqrs_input.json", "rb") as queue_file:
                    msgbody = queue_file.read()
                    mock_squeues.return_value = {
                        "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                    }
                    response = iqrs_wrangler.lambda_handler(
                        None,
                        {"aws_request_id": "666"},
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert """Bad data""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_incomplete_read(self):
        with mock.patch("iqrs_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("iqrs_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("iqrs_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 123456)
                    }
                    with open("iqrs_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = {
                            "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                        }
                        response = iqrs_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert """Incomplete Lambda response""" in response["error"]
