import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from botocore.response import StreamingBody
from moto import mock_lambda, mock_s3, mock_sns, mock_sqs

import apply_factors_method as lambda_method_function
import apply_factors_wrangler


class MockContext:
    aws_request_id = 666


mock_wrangles_event = {
  "MessageStructure": "json",
  "RuntimeVariables": {
    "period_column": "period",
    "calculation_type": "movement_calculation_b",
    "period": 201809,
    "id": "example",
    "distinct_values": "region",
    "raw_input_file": "non_responders_output.json",
    "sum_columns": [{"column_name": "Q608_total", "data": {
                    "Q603_concreting_sand": "+",
                    "Q602_building_soft_sand": "+"}}
                    ]
  }
}

context_object = MockContext()


class TestApplyFactors(unittest.TestCase):
    @mock_sqs
    def test_get_sqs(self):
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "method_name": "apply_factors_method",
                "period": "201809",
                "sqs_queue_url": "test-queue",
                "previous_data_file": "previous_period_enriched_stratared.json",
                "sqs_message_group_id": "apply_factors_out",
                "incoming_message_group": "Sheep",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                  "Q603_concreting_sand,Q604_bituminous_gravel," +
                                  "Q605_concreting_gravel,Q606_other_gravel," +
                                  "Q607_constructional_fill"
            },
        ):

            sqs = boto3.resource("sqs", region_name="eu-west-2")
            sqs.create_queue(QueueName="test-queue")
            sqs_queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

            messages = apply_factors_wrangler.aws_functions.get_sqs_message(sqs_queue_url)

            assert len(messages) == 1

    @mock_sqs
    @mock.patch("apply_factors_wrangler.aws_functions.get_dataframe")
    @mock.patch("apply_factors_wrangler.aws_functions.send_sns_message")
    @mock.patch("apply_factors_wrangler.aws_functions.save_to_s3")
    def test_sqs_messages_send(self, mock_me, mock_you, mock_everyone):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        queue = sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        apply_factors_wrangler.aws_functions.save_data("bucket_name", "Test",
                                                       "message", sqs_queue_url, "")

        messages = queue.receive_messages()
        assert len(messages) == 1

    @mock_sns
    def test_sns_send(self):
        with mock.patch.dict(apply_factors_wrangler.os.environ,
                             {"sns_topic_arn": "mike"}):
            sns = boto3.client("sns", region_name="eu-west-2")
            topic = sns.create_topic(Name="bloo")
            topic_arn = topic["TopicArn"]
            apply_factors_wrangler.aws_functions.send_sns_message("", topic_arn, "Gyargh")

    @mock_sqs
    def test_catch_wrangler_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "mike",
                "checkpoint": "3",
                "method_name": "lambda_method_function",
                "period": "201809",
                "sqs_queue_url": sqs_queue_url,
                "previous_data_file": "previous_period_enriched_stratared.json",
                "sqs_message_group_id": "apply_factors_out",
                "incoming_message_group": "Sheep",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                  "Q603_concreting_sand,Q604_bituminous_gravel," +
                                  "Q605_concreting_gravel,Q606_other_gravel," +
                                  "Q607_constructional_fill"
            },
        ):
            with mock.patch("apply_factors_wrangler"
                            ".aws_functions.read_dataframe_from_s3")\
                    as mocked:
                mocked.side_effect = Exception("SQS Failure")
                response = apply_factors_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )
                assert "success" in response
                assert response["success"] is False

    @mock_sqs
    def test_catch_method_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            apply_factors_wrangler.os.environ, {"sqs_queue_url": sqs_queue_url}
        ):
            with mock.patch("apply_factors_method.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                methodinput = pd.read_csv("tests/fixtures/inputtomethod.csv")
                mock_event = {
                    "json_data": json.loads(methodinput.to_json(orient="records")),
                    "questions_list": ["Q601_asphalting_sand", "Q602_building_soft_sand",
                                       "Q603_concreting_sand", "Q604_bituminous_gravel",
                                       "Q605_concreting_gravel", "Q606_other_gravel",
                                       "Q607_constructional_fill"],
                    "sum_columns": [{"column_name": "Q608_total", "data": {
                        "Q603_concreting_sand": "+",
                        "Q602_building_soft_sand": "+"}}]
                }
                response = lambda_method_function.lambda_handler(
                    mock_event, context_object
                )
                assert "success" in response
                assert response["success"] is False

    @mock_s3
    def test_get_data_from_s3(self):
        with mock.patch("apply_factors_wrangler.boto3") as mock_bot:
            mock_sthree = mock.Mock()
            mock_bot.resource.return_value = mock_sthree
            mock_object = mock.Mock()
            mock_sthree.Object.return_value = mock_object
            with open("tests/fixtures/test_data.json", "r") as file:
                mock_content = file.read()
            mock_object.get.return_value.read = mock_content
            data = pd.DataFrame(json.loads(mock_content))
            assert data.shape[0] == 8

    @mock_s3
    def test_get_data_from_s3_another_way(self):
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        s3 = boto3.resource(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        client.create_bucket(Bucket="MIKE")
        client.upload_file(
            Filename="tests/fixtures/factorsdata.json",
            Bucket="MIKE", Key="123"
        )

        s3object = s3.Object("MIKE", "123")
        content = s3object.get()["Body"].read()
        json_file = pd.DataFrame(json.loads(content))
        assert json_file.shape[0] == 14

    @mock_sqs
    @mock_s3
    @mock_lambda
    @mock.patch("apply_factors_wrangler.aws_functions.get_dataframe")
    @mock.patch("apply_factors_wrangler.aws_functions.send_sns_message")
    @mock.patch("apply_factors_wrangler.aws_functions.save_to_s3")
    def test_wrangles(self, mock_me, mock_you, mock_everyone):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test-queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

        with open("tests/fixtures/factorsdata.json", "r") as file:
            message = file.read()

            apply_factors_wrangler.aws_functions.save_data("bucket_name", "Test",
                                                           message, sqs_queue_url, "")
            # s3 bit
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="MIKE")
        client.upload_file(
            Filename="tests/fixtures/test_data.json",
            Bucket="MIKE",
            Key="previous_period_enriched_stratared.json",
        )
        client.upload_file(
            Filename="tests/fixtures/non_responders_output.json",
            Bucket="MIKE",
            Key="non_responders_output.json",
        )

        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "MIKE",
                "checkpoint": "3",
                "method_name": "apply_factors_method",
                "period": "201809",
                "sqs_queue_url": sqs_queue_url,
                "previous_data_file": "previous_period_enriched_stratared.json",
                "sqs_message_group_id": "apply_factors_out",
                "incoming_message_group": "Sheep",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                  "Q603_concreting_sand,Q604_bituminous_gravel," +
                                  "Q605_concreting_gravel,Q606_other_gravel," +
                                  "Q607_constructional_fill"
            },
        ):

            with mock.patch("apply_factors_wrangler.boto3.client") as mock_client:
                with mock.patch("apply_factors_wrangler.aws_functions") as mock_funk:
                    with open("tests/fixtures/non_responders_return.json", "r")\
                            as norespfile:
                        mock_client_object = mock.Mock()
                        mock_client.return_value = mock_client_object
                        mock_funk.get_dataframe.return_value = pd.DataFrame(
                            json.loads(message)), 666
                        mock_funk.read_dataframe_from_s3.return_value =\
                            pd.DataFrame(json.loads(norespfile.read()))
                        with open("tests/fixtures/apply_factors_return.json", "r")\
                                as file:

                            mock_client_object.invoke.return_value\
                                .get.return_value.read\
                                .return_value.decode.return_value = json.dumps({
                                    "data": file.read(), "success": True
                                })

                            response = apply_factors_wrangler.lambda_handler(
                                mock_wrangles_event, context_object)
                            assert "success" in response
                            assert response["success"] is True

    @mock_sqs
    def test_method(self):
        methodinput = pd.read_csv("tests/fixtures/inputtomethod.csv")
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {"sqs_queue_url": "Itsa Me! Queueio", "generic_var": "Itsa me, vario"},
        ):
            mock_event = {
                "json_data": json.loads(methodinput.to_json(orient="records")),
                # Note, sum columns here actually overwrites 608 total
                # but this way it checks both paths in sum_data_columns
                "sum_columns": [{"column_name": "Q608_total", "data": {
                                    "Q603_concreting_sand": "+",
                                    "Q602_building_soft_sand": "+"}},
                                {"column_name": "Q608_total", "data": {
                                    "Q603_concreting_sand": "+",
                                    "Q602_building_soft_sand": "-"}}
                                ],
                "questions_list": ["Q601_asphalting_sand", "Q602_building_soft_sand",
                                   "Q603_concreting_sand", "Q604_bituminous_gravel",
                                   "Q605_concreting_gravel", "Q606_other_gravel",
                                   "Q607_constructional_fill"]
            }
            response = lambda_method_function.lambda_handler(
                mock_event, context_object
            )

            outputdf = pd.read_json(response["data"])

            valuetotest = outputdf["Q608_total"].to_list()[0]
            assert valuetotest == 48293

    @mock_sqs
    def test_attribute_error_method(self):
        methodinput = {"potatoes": "seotatop"}
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {"sqs_queue_url": "Itsa Me! Queueio", "generic_var": "Itsa me, vario"},
        ):
            mock_event = {
                "json_data": json.dumps(methodinput),
                "sum_columns": [{"column_name": "test", "data": {
                    "Q601_asphalting_sand": "+", "Q602_building_soft_sand": "+"}}]
            }
            response = lambda_method_function.lambda_handler(
                mock_event, context_object
            )
            assert response["error"].__contains__("""Input Error""")

    @mock_sqs
    def test_key_error_method(self):
        methodinput = pd.read_csv("tests/fixtures/inputtomethod.csv")
        methodinput.rename(columns={"prev_Q601_asphalting_sand": "Mike"}, inplace=True)
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {"sqs_queue_url": "Itsa Me! Queueio", "generic_var": "Itsa me, vario"},
        ):
            response = lambda_method_function.lambda_handler(
                methodinput, context_object
            )
            assert response["error"].__contains__("""Key Error""")

    @mock_sqs
    def test_type_error_method(self):
        methodinput = pd.read_json("tests/fixtures/factorsdata.json")
        methodinput["prev_Q601_asphalting_sand"] = "MIKE"
        methodinput["imputation_factor_Q601_asphalting_sand"] = "MIIIKE!"
        mock_event = {
            "json_data": methodinput.to_json(orient="records"),
            "distinct_values": ["strata", "region"]
        }
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {"sqs_queue_url": "Itsa Me! Queueio", "generic_var": "Itsa me, vario"},
        ):
            response = lambda_method_function.lambda_handler(
                json.dumps(mock_event), context_object
            )
            assert response["error"].__contains__("""Bad Data type""")

    @mock_sqs
    def test_marshmallow_raises_wrangler_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {"checkpoint": "1", "sqs_queue_url": sqs_queue_url},
        ):
            out = apply_factors_wrangler.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, context_object
            )
            self.assertRaises(ValueError)
            assert out["error"].__contains__("""Error validating environment params""")

    @mock_sqs
    def test_wrangles_fail_to_get_from_sqs(self):
        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "MIKE",
                "checkpoint": "3",
                "method_name": "apply_factors_method",
                "raw_input_file": "non_responders_output.json",
                "period": "201809",
                "sqs_queue_url": "Sausages",
                "previous_data_file": "previous_period_enriched_stratared.json",
                "sqs_message_group_id": "apply_factors_out",
                "incoming_message_group": "Sheep",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                  "Q603_concreting_sand,Q604_bituminous_gravel," +
                                  "Q605_concreting_gravel,Q606_other_gravel," +
                                  "Q607_constructional_fill"
            },
        ):
            response = apply_factors_wrangler.lambda_handler(
                mock_wrangles_event, context_object
            )
            assert "success" in response
            assert response["success"] is False
            assert response["error"].__contains__("""AWS Error""")

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_wrangles_incomplete_data(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test-queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

        with open("tests/fixtures/factorsdata.json", "r") as file:
            message = file.read()
        with open("tests/fixtures/test_data.json", "r") as file:
            prevfile = pd.DataFrame(json.loads(file.read()))
        with open("tests/fixtures/non_responders_output.json", "r") as file:
            nonresponderfile = pd.DataFrame(json.loads(file.read()))

        with mock.patch.dict(
                apply_factors_wrangler.os.environ,
                {
                    "sns_topic_arn": "mike",
                    "bucket_name": "MIKE",
                    "checkpoint": "3",
                    "method_name": "apply_factors_method",
                    "raw_input_file": "non_responders_output.json",
                    "period": "201809",
                    "sqs_queue_url": sqs_queue_url,
                    "previous_data_file": "previous_period_enriched_stratared.json",
                    "sqs_message_group_id": "apply_factors_out",
                    "incoming_message_group": "Sheep",
                    "in_file_name": "Test",
                    "out_file_name": "Test",
                    "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                      "Q603_concreting_sand,Q604_bituminous_gravel," +
                                      "Q605_concreting_gravel,Q606_other_gravel," +
                                      "Q607_constructional_fill"
                },
        ):
            with mock.patch(
                    "apply_factors_wrangler.boto3.client") as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                with mock.patch("apply_factors_wrangler.aws_functions") as mock_funk:
                    mock_funk.get_dataframe.return_value = pd.DataFrame(
                        json.loads(message)), 666
                    mock_funk.read_dataframe_from_s3.side_effect = [
                        nonresponderfile, prevfile]
                    with open("tests/fixtures/non_responders_return.json",
                              "rb") as file:
                        mock_client_object.invoke.return_value = {
                            "Payload": StreamingBody(file, 1)
                        }
                        response = apply_factors_wrangler.lambda_handler(
                            mock_wrangles_event, context_object
                        )

                        assert "success" in response
                        assert response["success"] is False
                        assert response["error"].__contains__(
                            """Incomplete Lambda response"""
                        )

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_wrangles_key_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test-queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

        with open("tests/fixtures/factorsdata.json", "r") as file:
            message = file.read()

        with mock.patch.dict(
                apply_factors_wrangler.os.environ,
                {
                    "sns_topic_arn": "mike",
                    "bucket_name": "MIKE",
                    "checkpoint": "3",
                    "method_name": "apply_factors_method",
                    "raw_input_file": "non_responders_output.json",
                    "period": "201809",
                    "sqs_queue_url": sqs_queue_url,
                    "previous_data_file": "previous_period_enriched_stratared.json",
                    "sqs_message_group_id": "apply_factors_out",
                    "incoming_message_group": "Sheep",
                    "in_file_name": "Test",
                    "out_file_name": "Test",
                    "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                      "Q603_concreting_sand,Q604_bituminous_gravel," +
                                      "Q605_concreting_gravel,Q606_other_gravel," +
                                      "Q607_constructional_fill"
                },
        ):
            with mock.patch("apply_factors_wrangler.aws_functions") as mock_funk:
                mock_funk.get_dataframe.return_value = pd.DataFrame(
                    json.loads(message)), 666
                mock_funk.get_dataframe.side_effect = KeyError()
                response = apply_factors_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert response["error"].__contains__("""Key Error""")

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_wrangles_type_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test-queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "MIKE",
                "checkpoint": "3",
                "method_name": "apply_factors_method",
                "raw_input_file": "non_responders_output.json",
                "period": "201809",
                "sqs_queue_url": sqs_queue_url,
                "previous_data_file": "previous_period_enriched_stratared.json",
                "sqs_message_group_id": "apply_factors_out",
                "incoming_message_group": "Sheep",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                  "Q603_concreting_sand,Q604_bituminous_gravel," +
                                  "Q605_concreting_gravel,Q606_other_gravel," +
                                  "Q607_constructional_fill"
            },
        ):

            with mock.patch("apply_factors_wrangler.aws_functions") as mock_funk:
                mock_funk.get_dataframe.return_value = 66, 666

                response = apply_factors_wrangler.lambda_handler(
                    mock_wrangles_event, context_object
                )
                assert "success" in response
                assert response["success"] is False
                assert response["error"].__contains__("""Bad data type""")

    @mock_sqs
    @mock_s3
    @mock_lambda
    @mock.patch("apply_factors_wrangler.aws_functions.get_dataframe")
    @mock.patch("apply_factors_wrangler.aws_functions.send_sns_message")
    @mock.patch("apply_factors_wrangler.aws_functions.save_to_s3")
    @mock.patch("apply_factors_wrangler.aws_functions.read_dataframe_from_s3")
    def test_wrangles_method_fail(self, mock_read_dataframe_from_s3,
                                  mock_sns, mock_save_to_s3, mock_get_dataframe):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test-queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

        with open("tests/fixtures/factorsdata.json", "r") as file:
            message = file.read()

            apply_factors_wrangler.aws_functions.save_data("bucket_name", "Test",
                                                           message, sqs_queue_url, "")
            # s3 bit
        client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )

        client.create_bucket(Bucket="MIKE")
        client.upload_file(
            Filename="tests/fixtures/test_data.json",
            Bucket="MIKE",
            Key="previous_period_enriched_stratared.json",
        )
        client.upload_file(
            Filename="tests/fixtures/non_responders_output.json",
            Bucket="MIKE",
            Key="non_responders_output.json",
        )

        with mock.patch.dict(
            apply_factors_wrangler.os.environ,
            {
                "sns_topic_arn": "mike",
                "bucket_name": "MIKE",
                "checkpoint": "3",
                "method_name": "apply_factors_method",
                "raw_input_file": "non_responders_output.json",
                "period": "201809",
                "sqs_queue_url": sqs_queue_url,
                "previous_data_file": "previous_period_enriched_stratared.json",
                "sqs_message_group_id": "apply_factors_out",
                "incoming_message_group": "Sheep",
                "in_file_name": "Test",
                "out_file_name": "Test",
                "questions_list": "Q601_asphalting_sand,Q602_building_soft_sand," +
                                  "Q603_concreting_sand,Q604_bituminous_gravel," +
                                  "Q605_concreting_gravel,Q606_other_gravel," +
                                  "Q607_constructional_fill"
            },
        ):

            with mock.patch("apply_factors_wrangler.boto3.client") as mock_client:

                with open("tests/fixtures/non_responders_return.json", "r")\
                        as norespfile:
                    mock_client_object = mock.Mock()
                    mock_client.return_value = mock_client_object
                    mock_read_dataframe_from_s3.return_value = \
                        pd.DataFrame(json.loads(norespfile.read()))
                    mock_get_dataframe.return_value = \
                        pd.DataFrame(json.loads(message)), 666

                    mock_client_object.invoke.return_value.get.return_value\
                        .read.return_value.decode.return_value =\
                        json.dumps({"success": False,
                                    "error": "This is an error message"})

                    response = apply_factors_wrangler.lambda_handler(
                        mock_wrangles_event, context_object)
                    assert "success" in response
                    assert response["success"] is False
                    assert "error message" in response["error"]
