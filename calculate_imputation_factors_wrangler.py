import json
import traceback
import random
import os
import pandas as pd
import boto3

# Set up clients
s3 = boto3.resource('s3')
sqs = boto3.client('sqs')
lambda_client = boto3.client('lambda')
sns = boto3.client('sns')

# Sqs
queue_url = os.environ['queue_url']
sqs_messageid_name = os.environ['sqs_messageid_name']

# Sns
arn = os.environ['arn']
checkpoint = os.environ['checkpoint']

current_period = os.environ['period']
method_name = os.environ['method_name']
questions = os.environ['questions']


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """

    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def lambda_handler(event, context):
    try:
        response = sqs.receive_message(QueueUrl=queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])
        data = pd.DataFrame(message_json)

        for question in questions.split(' '):
            data['imputation_factor_' + question] = 0.0

        data_json = data.to_json(orient='records')

        calculate_factors = lambda_client.invoke(FunctionName=method_name, Payload=data_json)
        json_response = calculate_factors.get('Payload').read().decode("UTF-8")

        send_sqs_message(queue_url, json.loads(json_response), sqs_messageid_name)

    except Exception as exc:

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }


def send_sns_message(imputation_run_type, anomalies):
    sns_message = {
        "success": True,
        "module": "Imputation Calculate Imputation Factors",
        "checkpoint": checkpoint,
        "anomalies": anomalies.to_json(orient='records'),
        "message": imputation_run_type
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def send_sqs_message(queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.
    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """
    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128))
                     )
