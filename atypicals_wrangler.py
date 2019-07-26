import json
import boto3
import traceback
import pandas as pd
import os
import random
import marshmallow


class InputSchema(marshmallow.Schema):
    queue_url = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)
    arn = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    atypical_columns = marshmallow.fields.Str(required=True)
    method_name = marshmallow.fields.Str(required=True)


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
    # clients
    sqs = boto3.client('sqs')
    lambda_client = boto3.client('lambda')
    sns = boto3.client('sns')

    # env vars
    config, errors = InputSchema().load(os.environ)
    if errors:
        raise ValueError(f"Error validating environment params: {errors}")

    try:

        #  Reads in Data from SQS Queue
        response = sqs.receive_message(QueueUrl=config['queue_url'])
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        # Used for clearing the Queue
        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)
        for col in config['atypical_columns'].split(','):
            data[col] = 0

        data_json = data.to_json(orient='records')

        wrangled_data = lambda_client.invoke(
            FunctionName=config['method_name'],
            Payload=json.dumps(data_json)
        )

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")
        sqs.send_message(
            QueueUrl=config['queue_url'],
            MessageBody=json_response,
            MessageGroupId=config['sqs_messageif_name'],
            MessageDeduplicationId=str(random.getrandbits(128))
        )

        sqs.delete_message(QueueUrl=config['queue_url'], ReceiptHandle=receipt_handle)

        send_sns_message(config['arn'], sns, config['checkpoint'])

    except Exception as exc:

        return {
            "success": False,
            "checkpoint": config['checkpoint'],
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": config['checkpoint']
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
