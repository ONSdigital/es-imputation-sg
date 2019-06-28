
import os
from moto import mock_sqs, mock_sns, mock_s3, mock_lambda
import boto3
import json
import unittest.mock as mock
import unittest
import pandas as pd
import uk.gov.ons.src.lambda_wrangler_function as lambda_wrangler_function
import uk.gov.ons.src.lambda_method_function as lambda_method_function
import sys
sys.path.append(os.path.realpath(os.path.dirname(__file__)+"/.."))


class test_wrangler_handler(unittest.TestCase):

    @mock_sqs
    def test_get_sqs(self):
        with mock.patch.dict(lambda_wrangler_function.os.environ, {
            'arn': 'mike',
            'bucket_name': 'mike',
            'checkpoint': '3',
            'method_name': 'lambda_method_function',
            'non_responder_file': 'non_responders_output.json',
            'period': '201809',
            'queue_url': 'test-queue',
            's3_file': 'previous_period_enriched_stratared.json',
            'sqs_messageid_name': 'apply_factors_out'
        }):

            sqs = boto3.resource('sqs', region_name='eu-west-2')
            sqs.create_queue(QueueName="test-queue")
            queue_url = sqs.get_queue_by_name(QueueName="test-queue").url

            messages = lambda_wrangler_function.recieve_message(queue_url)

            assert len(messages) == 1

    @mock_sqs
    def test_sqs_messages_send(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        queue = sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        lambda_wrangler_function.send_sqs_message(queue_url, "", "")

        messages = queue.receive_messages()
        assert len(messages) == 1

    @mock_sns
    def test_sns_send(self):
        with mock.patch.dict(lambda_wrangler_function.os.environ, {
               'arn': 'mike'}):
            sns = boto3.client('sns', region_name='eu-west-2')
            topic = sns.create_topic(Name='bloo')
            topic_arn = topic['TopicArn']
            lambda_wrangler_function.send_sns_message(topic_arn,"Gyargh",3)

    # Testing SQS functionality - Method
    def test_catch_exception(self):
        # Method
        with mock.patch.dict(lambda_wrangler_function.os.environ, {
            'arn': 'mike',
            'bucket_name': 'mike',
            'checkpoint': '3',
            'method_name': 'lambda_method_function',
            'non_responder_file': 'non_responders_output.json',
            'period': '201809',
            'queue_url': 'test-queue',
            's3_file': 'previous_period_enriched_stratared.json',
            'sqs_messageid_name': 'apply_factors_out'
        }):
            with mock.patch('uk.gov.ons.src.lambda_wrangler_function.boto3') as mocked:
                mocked.client.side_effect = Exception('SQS Failure')
                response = lambda_wrangler_function.lambda_handler("", None)
                assert 'success' in response
                assert response['success'] is False

    @mock_s3
    def test_get_data_from_s3(self):
        with mock.patch('uk.gov.ons.src.lambda_wrangler_function.boto3') as mock_bot:
            mock_sthree = mock.Mock()
            mock_bot.resource.return_value = mock_sthree
            mock_object = mock.Mock()
            mock_sthree.Object.return_value = mock_object
            with open('test_data.json', "r") as file:
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
        client.upload_file(Filename="factorsdata.json", Bucket="MIKE", Key="123")

        object = s3.Object("MIKE", "123")
        content = object.get()['Body'].read()
        json_file = pd.DataFrame(json.loads(content))
        assert json_file.shape[0] == 14

    @mock_sqs
    @mock_s3
    @mock_lambda
    def test_wrangles(self):
        sqs = boto3.resource('sqs', region_name='eu-west-2')
        sqs.create_queue(QueueName="test-queue")
        queue_url = sqs.get_queue_by_name(QueueName="test-queue").url
        message = ''
        testdata = ''
        with open('factorsdata.json', "r") as file:
            message = file.read()
        with open('test_data.json', "r") as file:
            testdata = file.read()

            lambda_wrangler_function.send_sqs_message(queue_url, message, "testy")
            #s3 bit
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
        client.upload_file(Filename="test_data.json", Bucket="MIKE", Key="previous_period_enriched_stratared.json")
        client.upload_file(Filename="non_responders_output.json", Bucket="MIKE", Key="non_responders_output.json")

        with mock.patch.dict(lambda_wrangler_function.os.environ, {
            'arn': 'mike',
            'bucket_name': 'MIKE',
            'checkpoint': '3',
            'method_name': 'lambda_method_function',
            'non_responder_file': 'non_responders_output.json',
            'period': '201809',
            'queue_url': queue_url,
            's3_file': 'previous_period_enriched_stratared.json',
            'sqs_messageid_name': 'apply_factors_out'
        }):
            from botocore.response import StreamingBody
            with mock.patch('uk.gov.ons.src.lambda_wrangler_function.boto3.client') as mock_client:
                mock_client_object = mock.Mock()
                mock_client.return_value = mock_client_object
                mock_client_object.receive_message.return_value = {"Messages": [{"Body": message}]}
                myvar = mock_client_object.send_message.call_args_list
                with open('non_responders_output.json', "rb") as file:
                    mock_client_object.invoke.return_value = {"Payload": StreamingBody(file, 1317)}
                    response = lambda_wrangler_function.lambda_handler("", None)
                    output = myvar[0][1]['MessageBody']
                    outputdf = pd.DataFrame(json.loads(output))
                    outputdf = outputdf[outputdf['response_type'] == 1]
                    a_value_to_test = outputdf['Q603_concreting_sand'].to_list()[0]
                    assert a_value_to_test == 91
                    assert 'success' in response
                    assert response['success'] is True

    def test_method(self):
        input = pd.read_csv('inputtomethod.csv')
        response = lambda_method_function.lambda_handler(input, None)
        outputdf = pd.DataFrame(json.loads(response))
        valuetotest = outputdf['Q602_building_soft_sand'].to_list()[0]
        assert valuetotest == 4659

    def test_get_traceback(self):
        traceback = lambda_wrangler_function._get_traceback(Exception('Mike'))
        assert traceback == 'Exception: Mike\n'
