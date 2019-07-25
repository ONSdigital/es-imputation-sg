import json
import boto3
import traceback
import pandas as pd
import os
import random


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
    sqs = boto3.client('sqs')
    lambda_client = boto3.client('lambda')
    sns = boto3.client('sns')

    # Sqss
    queue_url = os.environ['queue_url']
    sqs_messageid_name = os.environ['sqs_messageid_name']

    # Sns
    arn = os.environ['arn']
    checkpoint = os.environ['checkpoint']

    atypical_columns = os.environ['atypical_columns']

    method_name = os.environ['method_name']

    # bucket_name = os.environ['bucket_name']
    input_data = os.environ['input_data']

    try:

        #  Reads in Data from SQS Queue
        response = sqs.receive_message(QueueUrl=queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        # Reads in data from S3 Bucket into a JSON file

        # content_object = s3.Object(bucket_name, input_data)
        # json_content = content_object.get()['Body'].read().decode('utf-8')

        # Used for clearing the Queue
        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)
        for col in atypical_columns.split(','):
            data[col] = 0

        data_json = data.to_json(orient='records')

        wrangled_data = lambda_client.invoke(FunctionName=method_name,
                                              Payload=json.dumps(data_json))

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")
        sqs.send_message(QueueUrl=queue_url, MessageBody=json_response, MessageGroupId=sqs_messageid_name,
                         MessageDeduplicationId=str(random.getrandbits(128)))

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        send_sns_message(arn, sns, checkpoint)

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


def send_sns_message(arn, sns, checkpoint):
    sns_message = {
        "success": True,
        "module": "outlier_aggregation",
        "checkpoint": checkpoint
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )
