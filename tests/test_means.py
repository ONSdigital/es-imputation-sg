import json
import unittest
import unittest.mock as mock
import boto3
import pandas as pd
import calculate_means_method
import calculate_means_wrangler
from pandas.util.testing import assert_frame_equal
from moto import mock_sqs, mock_lambda
from botocore.response import StreamingBody


class TestMeans(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                # 'checkpoint': '3',
                "error_handler_arn": "mock_arn",
                "sqs_messageid_name": "mock_message",
                "checkpoint": "mock_checkpoint",
                "function_name": "mock_method",
                "queue_url": "mock_queue",
                "questions_list": "Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel Q607_constructional_fill",  # noqa: E501
                "movement_columns": "movement_Q601_asphalting_sand movement_Q602_building_soft_sand movement_Q603_concreting_sand movement_Q604_bituminous_gravel movement_Q605_concreting_gravel movement_Q606_other_gravel movement_Q607_constructional_fill region strata",  # noqa: E501
                "current_period": "mock_period",
                "previous_period": "mock_prev_period",
                "arn": "mock_arn",
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        # Stop the mocking of the os stuff
        cls.mock_os_patcher.stop()

    @mock_sqs
    @mock_lambda
    def test_wrangler_happy_path(self):
        with mock.patch("calculate_means_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("means_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 226388)
                    }
                    with open("means_input.json", "rb") as queue_file:
                        msgbody = queue_file.read()
                        mock_squeues.return_value = {
                            "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                        }
                        response = calculate_means_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"},
                        )

                        assert "success" in response
                        assert response["success"] is True

    def test_method_happy_path(self):
        input_file = "mean_input_with_columns.json"
        with open(input_file, "r") as file:
            mean_col = "mean_Q601_asphalting_sand,mean_Q602_building_soft_sand,mean_Q603_concreting_sand,mean_Q604_bituminous_gravel,mean_Q605_concreting_gravel,mean_Q606_other_gravel,mean_Q607_constructional_fill"  # noqa: E501
            sorting_cols = ["responder_id", "region", "strata"]
            selected_cols = mean_col.split(",")

            json_content = json.loads(file.read())
            output = calculate_means_method.lambda_handler(
                json_content,
                {"aws_request_id": "666"}
            )

            expected_df = (
                pd.read_csv("means_output.csv")
                .sort_values(sorting_cols)
                .reset_index()[selected_cols]
            )

            response_df = (
                pd.read_json(output)
                .sort_values(sorting_cols)
                .reset_index()[selected_cols]
            )

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    @mock.patch("calculate_means_wrangler.boto3")
    def test_wrangler_general_exception(self, mock_boto):
        with mock.patch("calculate_means_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("means_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 226388)
                    }
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = {
                        "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                    }
                    with mock.patch("calculate_means_wrangler.pd.DataFrame") as mocked:
                        mocked.side_effect = Exception("General exception")
                        response = calculate_means_wrangler.lambda_handler(
                            None,
                            {"aws_request_id": "666"}
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert """General exception""" in response["error"]

    def test_method_general_exception(self):
        input_file = "mean_input_with_columns.json"
        with open(input_file, "r") as file:
            json_content = json.loads(file.read())
            with mock.patch("calculate_means_method.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("General exception")
                response = calculate_means_method.lambda_handler(
                    json_content,
                    {"aws_request_id": "666"}
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangler_key_error(self):
        with mock.patch("calculate_means_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with open("means_input.json", "rb") as file:
                    mock_client_object.invoke.return_value = {
                        "Payload": StreamingBody(file, 226388)
                    }
                    msgbody = '{"period": 201809}'
                    mock_squeues.return_value = {
                        "Messages": [{"Sausages": msgbody, "ReceiptHandle": 666}]
                    }
                    response = calculate_means_wrangler.lambda_handler(
                        None,
                        {"aws_request_id": "666"},
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert """Key Error""" in response["error"]

    def test_method_key_error(self):
        # pass none value to trigger key index error
        response = calculate_means_method.lambda_handler(None, {"aws_request_id": "666"})
        assert """Key Error""" in response["error"]

    def test_marshmallow_raises_wrangler_exception(self):
        """
        Testing the marshmallow raises an exception in wrangler.
        :return: None.
        """
        # Removing the strata_column to allow for test of missing parameter
        calculate_means_wrangler.os.environ.pop("function_name")
        response = calculate_means_wrangler.lambda_handler(None, {"aws_request_id": "666"})  # noqa E501
        calculate_means_wrangler.os.environ["function_name"] = "mock_method"
        assert """Error validating environment params:""" in response["error"]

    def test_marshmallow_raises_method_exception(self):
        """
        Testing the marshmallow raises an exception in method.
        :return: None.
        """
        input_file = "mean_input_with_columns.json"
        with open(input_file, "r") as file:
            json_content = json.loads(file.read())
            # Removing movement_columns to allow for test of missing parameter
            calculate_means_method.os.environ.pop("movement_columns")
            response = calculate_means_method.lambda_handler(json_content, {"aws_request_id": "666"})  # noqa E501
            calculate_means_method.os.environ["movement_columns"] = "movement_Q601_asphalting_sand movement_Q602_building_soft_sand movement_Q603_concreting_sand movement_Q604_bituminous_gravel movement_Q605_concreting_gravel movement_Q606_other_gravel movement_Q607_constructional_fill region strata"  # noqa E501
            assert """Error validating environment params:""" in response["error"]

    @mock_sqs
    def test_no_data_in_queue(self):
        sqs = boto3.client("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_url(QueueName="test_queue")["QueueUrl"]
        with mock.patch.dict(
            calculate_means_wrangler.os.environ,
            {
                "queue_url": queue_url
            },
        ):
            response = calculate_means_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """There was no data in sqs queue in""" in response["error"]

    @mock_sqs
    def test_wrangler_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            calculate_means_wrangler.os.environ,
            {
                "queue_url": "An Invalid Queue"
            },
        ):
            response = calculate_means_wrangler.lambda_handler(
                None, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """AWS Error""" in response["error"]

    @mock_sqs
    @mock_lambda
    def test_wrangles_bad_data(self):
        with mock.patch("calculate_means_wrangler.get_sqs_message") as mock_squeues:
            with mock.patch("calculate_means_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.invoke.return_value = {
                    "Payload": StreamingBody("{'boo':'moo':}", 2)
                }
                with open("means_input.json", "rb") as queue_file:
                    msgbody = queue_file.read()
                    mock_squeues.return_value = {
                        "Messages": [{"Body": msgbody, "ReceiptHandle": 666}]
                    }
                    response = calculate_means_wrangler.lambda_handler(
                        None,
                        {"aws_request_id": "666"},
                    )

                    assert "success" in response
                    assert response["success"] is False
                    assert """Bad data""" in response["error"]
