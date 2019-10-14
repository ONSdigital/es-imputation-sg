import json
import os
import sys
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from moto import mock_sns, mock_sqs
from pandas.util.testing import assert_frame_equal

import calculate_imputation_factors_method  # noqa E402
import calculate_imputation_factors_wrangler  # noqa E402

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))


class TestWranglerAndMethod(unittest.TestCase):
    """
    Tests the wrangler and method lambdas integration.
    """

    @classmethod
    def setup_class(cls):
        """
        sets up the mock boto clients and starts the patchers.
        :return: None.

        """
        # setting up the wrangler mock boto patcher and starting the patch
        cls.mock_boto_wrangler_patcher = mock.patch(
            "calculate_imputation_factors_wrangler.boto3"
        )
        cls.mock_boto_wrangler = cls.mock_boto_wrangler_patcher.start()

        # setting up the method mock boto patcher and starting the patch
        cls.mock_boto_method_patcher = mock.patch(
            "calculate_imputation_factors_method.boto3"
        )
        cls.mock_boto_method = cls.mock_boto_method_patcher.start()

        # setting up the mock environment variables for the wrangler
        cls.mock_os_wrangler_patcher = mock.patch.dict(
            "os.environ",
            {
                "arn": "mock_arn",
                "checkpoint": "mock_checkpoint",
                "method_name": "mock_method",
                "period": "mock_period",
                "questions": "Q601_asphalting_sand Q602_building_soft_sand "
                             "Q603_concreting_sand "
                + "Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel"
                + " Q607_constructional_fill",
                "queue_url": "mock_queue",
                "sqs_messageid_name": "mock_message",
            },
        )

        # setting up the mock environment variables for the method
        cls.mock_os_method_patcher = mock.patch.dict(
            "os.environ",
            {
                "arn": "mock_arn",
                "checkpoint": "mock_checkpoint",
                "first_imputation_factor": str(1),
                "first_threshold": str(7),
                "period": "mock_period",
                "questions": "Q601_asphalting_sand Q602_building_soft_sand "
                             "Q603_concreting_sand "
                + "Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel "
                + "Q607_constructional_fill",
                "queue_url": "mock_queue",
                "second_imputation_factor": str(2),
                "second_threshold": str(7),
                "third_imputation_factor": str(1),
                "third_threshold": str(9),
            },
        )
        cls.mock_os_w = cls.mock_os_wrangler_patcher.start()
        cls.mock_os_m = cls.mock_os_method_patcher.start()

    @classmethod
    def teardown_class(cls):
        """
        stops the wrangler, method and os patchers.
        :return: None.

        """
        cls.mock_boto_wrangler_patcher.stop()
        cls.mock_boto_method_patcher.stop()
        cls.mock_os_wrangler_patcher.stop()
        cls.mock_os_method_patcher.stop()

    @mock_sns
    @mock_sqs
    def test_wrangler(self):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        # load json file.

        method_input_file = (
            "tests/fixtures/calculate_imputation_factors_method_input_data.json"
        )

        with open(method_input_file, "r") as file:
            json_content = file.read()

        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ, {"queue_url": queue_url}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                mocked_client = mock.Mock()
                mocked.return_value = mocked_client
                mocked_client.receive_message.return_value = {
                    "Messages": [{"Body": json_content, "ReceiptHandle": "LORD MIKE"}]
                }
                with open(method_input_file, "rb") as file:
                    invoke_return = file

                    mocked_client.invoke.return_value = {
                        "Payload": StreamingBody(invoke_return, 2760)
                    }

                    out = calculate_imputation_factors_wrangler.lambda_handler(
                        None, {"aws_request_id": "666"}
                    )
                    assert "success" in out
                    assert out["success"]

    @mock_sqs
    @mock_sns
    def test_method(self):
        """
        mocks functionality of the method
        :return: None
        """
        # read in the json payload from wrangler
        input_file = "tests/fixtures/calculate_imputation_factors_method_input_data.json"

        with open(input_file, "r") as file:
            json_content = json.loads(file.read())

        output_file = calculate_imputation_factors_method.lambda_handler(
            json_content, None
        )

        # final output match
        expected_output_file = (
            "tests/fixtures/calculate_imputation_factors_method_output.json"
        )
        with open(expected_output_file, "r") as file:
            expected_dataframe = json.loads(file.read())

        actual_outcome_dataframe = pd.DataFrame(json.loads(output_file))
        expected_output_dataframe = pd.DataFrame(expected_dataframe)
        assert_frame_equal(
            actual_outcome_dataframe.astype(str), expected_output_dataframe.astype(str)
        )

    @mock_sqs
    def test_wrangler_no_data_in_queue(self):
        """
        testing the exception handler works within the wrangler.
        :param self:
        :return: mock response
        """
        response = calculate_imputation_factors_wrangler.lambda_handler(
            None, {"aws_request_id": "666"}
        )
        assert not response["success"]
        assert response["error"].__contains__("no data in sqs queue")

    def test_method_exception_handling(self):
        """
        testing the exception handler within the method.
        :param self:
        :return: mock response
        """

        json_data_content = (
            '[{"movement_Q601_asphalting_sand":0.0},'
            '{"movement_Q601_asphalting_sand":0.857614899}]'
        )

        response = calculate_imputation_factors_method.lambda_handler(
            json_data_content, {"aws_request_id": "666"}
        )
        assert not response["success"]

    def test_raise_general_exception_wrangles(self):
        with mock.patch("calculate_imputation_factors_wrangler.boto3.client") as mocked:
            mocked.side_effect = Exception("AARRRRGHH!!")
            response = calculate_imputation_factors_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """AARRRRGHH!!""" in response["error"]

    def test_raise_general_exception_method(self):
        with mock.patch("pandas.DataFrame") as mocked:
            mocked.side_effect = Exception("AARRRRGHH!!")
            response = calculate_imputation_factors_method.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
            )
            assert "success" in response
            assert response["success"] is False
            assert """AARRRRGHH!!""" in response["error"]

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ, {"queue_url": queue_url}
        ):
            calculate_imputation_factors_wrangler.os.environ.pop("method_name")
            out = calculate_imputation_factors_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
            )
            self.assertRaises(ValueError)
            print(out)
            assert """Parameter validation error""" in out["error"]

    @mock_sqs
    def test_marshmallow_raises_method_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            calculate_imputation_factors_method.os.environ, {"queue_url": queue_url}
        ):
            calculate_imputation_factors_method.os.environ.pop("questions")
            out = calculate_imputation_factors_method.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, {"aws_request_id": "666"}
            )
            calculate_imputation_factors_method.os.environ[
                "questions"
            ] = "Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand" \
                " Q604_bituminous_gravel Q605_concreting_gravel Q606_other_gravel " \
                "Q607_constructional_fill"
            self.assertRaises(ValueError)

            assert """Parameter validation error""" in out["error"]

    def test_method_key_error_exception(self):
        """
        mocks functionality of the method
        :return: None
        """
        # read in the json payload from wrangler
        input_file = "tests/fixtures/calculate_imputation_factors_method_input_data.json"

        with open(input_file, "r") as file:
            content = file.read()
            content = content.replace("Q", "MIKE")
            json_content = json.loads(content)

        print(json_content)
        output_file = calculate_imputation_factors_method.lambda_handler(
            json_content, {"aws_request_id": "666"}
        )

        assert not output_file["success"]
        assert "Key Error" in output_file["error"]

    @mock_sns
    @mock_sqs
    def test_wrangler_incomplete_read_error(self):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        # load json file.

        method_input_file = (
            "tests/fixtures/calculate_imputation_factors_method_input_data.json"
        )

        with open(method_input_file, "r") as file:
            json_content = file.read()

        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ, {"queue_url": queue_url}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                mocked_client = mock.Mock()
                mocked.return_value = mocked_client
                print(json_content)
                mocked_client.receive_message.return_value = {
                    "Messages": [{"Body": json_content, "ReceiptHandle": "LORD MIKE"}]
                }
                with open(method_input_file, "rb") as file:
                    invoke_return = file

                    mocked_client.invoke.return_value = {
                        "Payload": StreamingBody(invoke_return, 666)
                    }

                    out = calculate_imputation_factors_wrangler.lambda_handler(
                        None, {"aws_request_id": "666"}
                    )
                    assert "success" in out
                    assert not out["success"]
                    assert "Incomplete Lambda response" in out["error"]

    @mock_sns
    @mock_sqs
    def test_wrangler_key_error(self):
        method_input_file = (
            "tests/fixtures/calculate_imputation_factors_method_input_data.json"
        )
        with open(method_input_file, "r") as file:
            json_content = file.read()

        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ, {"queue_url": queue_url}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                mocked_client = mock.Mock()
                mocked.return_value = mocked_client
                mocked_client.receive_message.return_value = {
                    "Messages": [{"LordMike": json_content, "ReceiptHandle": "LORD MIKE"}]
                }
                out = calculate_imputation_factors_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )
                assert "success" in out
                assert not out["success"]
                assert "Key Error" in out["error"]

    def test_wrangler_client_error(self):
        with mock.patch.dict(
            calculate_imputation_factors_wrangler.os.environ, {"queue_url": "mike"}
        ):
            with mock.patch(
                "calculate_imputation_factors_wrangler.boto3.client"
            ) as mocked:
                mocked.side_effect = ClientError(
                    {"Error": {"Code": "Mike"}}, "create_stream"
                )
                out = calculate_imputation_factors_wrangler.lambda_handler(
                    None, {"aws_request_id": "666"}
                )
                assert "success" in out
                assert not out["success"]
                assert "AWS Error" in out["error"]
