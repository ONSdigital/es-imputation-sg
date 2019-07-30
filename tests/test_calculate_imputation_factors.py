import unittest
import unittest.mock as mock
import json
import pandas as pd
from pandas.util.testing import assert_frame_equal
import sys
import os
from moto import mock_sqs, mock_sns
import boto3

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
import calculate_imputation_factors_wrangler  # noqa E402
import calculate_imputation_factors_method  # noqa E402


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
                "questions": "Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand "
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
                "questions": "Q601_asphalting_sand Q602_building_soft_sand Q603_concreting_sand "
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

    def test_wrangler(self):
        """
        mocks functionality of the wrangler:
        - load json file. (uses the calc_imps_test_data.json file)
        - invoke the method lambda.
        - retrieve the payload from the method.
        :return: None.

        """
        # load json file.

        method_input_file = "tests/fixtures/calculate_imputation_factors_method_input_data.json"

        with open(method_input_file, "r") as file:
            json_content = json.loads(file.read())

        # invoke method lambda
        with mock.patch("json.loads") as mock_json:
            mock_json.return_value = json_content
            mocked_client = mock.Mock()
            self.mock_boto_wrangler.client.return_value.invoke = mocked_client
            # retrieve payload from method lambda
            calculate_imputation_factors_wrangler.lambda_handler(None, None)

        with open(method_input_file, "r") as file:
            payload_method = json.load(file)
        # check the output file contains the expected columns and non null values
        payload_dataframe = pd.DataFrame(payload_method)
        required_columns = {
            "imputation_factor_Q601_asphalting_sand",
            "imputation_factor_Q602_building_soft_sand",
            "imputation_factor_Q603_concreting_sand",
            "imputation_factor_Q604_bituminous_gravel",
            "imputation_factor_Q605_concreting_gravel",
            "imputation_factor_Q606_other_gravel",
            "imputation_factor_Q607_constructional_fill",
        }

        self.assertTrue(
            required_columns.issubset(set(payload_dataframe.columns)),
            "Calculate Imputation Columns not in the DataFrame.",
        )
        new_columns = payload_dataframe[required_columns]

        self.assertFalse(new_columns.isnull().values.any())

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
        expected_output_file = "tests/fixtures/calculate_imputation_factors_method_output.json"
        with open(expected_output_file, "r") as file:
            expected_dataframe = json.loads(file.read())

        actual_outcome_dataframe = pd.DataFrame(json.loads(output_file))
        expected_output_dataframe = pd.DataFrame(expected_dataframe)
        print(actual_outcome_dataframe)
        print(expected_output_dataframe)
        assert_frame_equal(
            actual_outcome_dataframe.astype(str), expected_output_dataframe.astype(str)
        )

    def test_wrangler_exception_handling(self):
        """
        testing the exception handler works within the wrangler.
        :param self:
        :return: mock response
        """
        response = calculate_imputation_factors_wrangler.lambda_handler(None, None)
        assert not response["success"]

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
            json_data_content, None
        )
        assert not response["success"]

    def test_get_traceback(self):
        traceback = calculate_imputation_factors_wrangler._get_traceback(
            Exception("Mike")
        )
        assert traceback == "Exception: Mike\n"

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
                {"RuntimeVariables": {"checkpoint": 666}}, None
            )
            self.assertRaises(ValueError)
            print(out)
            assert out["error"].__contains__(
                """ValueError: Error validating environment params:"""
            )

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
                {"RuntimeVariables": {"checkpoint": 666}}, None
            )
            self.assertRaises(ValueError)

            assert out["error"].__contains__(
                """ValueError: Error validating environment params:"""
            )
