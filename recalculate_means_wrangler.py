import traceback
import json
import random
import os
import boto3
import pandas as pd


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
    # Set up clients
    sqs = boto3.client('sqs', 'eu-west-2')
    sns = boto3.client('sns', 'eu-west-2')
    lambda_client = boto3.client('lambda', 'eu-west-2')

    # ENV vars
    error_handler_arn = os.environ['error_handler_arn']
    queue_url = os.environ['queue_url']
    checkpoint = os.environ['checkpoint']
    function_name = os.environ['function_name']
    questions_list = os.environ['questions_list']
    sqs_messageid_name = os.environ['sqs_messageid_name']

    try:

        response = sqs.receive_message(QueueUrl=queue_url)
        message = response['Messages'][0]
        message_json = json.loads(message['Body'])

        receipt_handle = message['ReceiptHandle']

        data = pd.DataFrame(message_json)
        # Add means columns
        qno = 1
        for question in questions_list.split(' '):
            data.drop(['movement_' + question + '_count'], axis=1, inplace=True)
            data.drop(['movement_' + question + '_sum'], axis=1, inplace=True)
            data.drop(['atyp60' + str(qno), "iqrs60" + str(qno)], axis=1, inplace=True)
            qno += 1
            data['mean_' + question] = 0.0

        data_json = data.to_json(orient='records')

        returned_data = lambda_client.invoke(FunctionName=function_name, Payload=data_json)
        json_response = returned_data.get('Payload').read().decode("UTF-8")

        # MessageDeduplicationId is set to a random hash to overcome de-duplication,
        # otherwise modules could not be re-run in the space of 5 Minutes.
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.loads(json_response),
                         MessageGroupId=sqs_messageid_name,
                         MessageDeduplicationId=str(random.getrandbits(128)))

        ### COMMENTED OUT FOR TESTING ###
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        final_output = json_response

    except Exception as exc:
        print("Unexpected exception {}".format(_get_traceback(exc)))
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
    """
    This function is responsible for sending notifications to the SNS Topic.
    :param imputation_run_type: runtype.
    :param anomalies: list of anomalies collected from the method.
    :return: json string
    """

    arn = os.environ['arn']
    checkpoint = os.environ['checkpoint']
    sns = boto3.client('sns')

    sns_message = {
        "success": True,
        "module": "Imputation ReCalculate Means",
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
    sqs = boto3.client('sqs')

    sqs.send_message(QueueUrl=queue_url,
                     MessageBody=message,
                     MessageGroupId=output_message_id,
                     MessageDeduplicationId=str(random.getrandbits(128)))
