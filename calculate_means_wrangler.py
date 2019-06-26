import traceback
import json
import boto3
import pandas as pd
import os
import random

# Set up clients
sqs = boto3.client('sqs', region_name='eu-west-2')
sns = boto3.client('sns', region_name='eu-west-2')
lambda_client = boto3.client('lambda', region_name='eu-west-2')

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


def send_sns_message(arn, imputation_run_type, anomalies):
    sns_message = {
        "success": True,
        "module": "Imputation",
        "checkpoint": "3",
        "anomalies": anomalies,
        "message": imputation_run_type
    }

    sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def _get_sqs_message():
    # Reads in Data from SQS Queue
    response = sqs.receive_message(QueueUrl=queue_url)
    message = response['Messages'][0]
    message_json = json.loads(message['Body'])
    receipt_handle = message['ReceiptHandle']

    return message_json, receipt_handle


def send_sqs_message(queue_url, message, message_group_id, message_dedup_id):
    # MessageDeduplicationId is set to a random hash to overcome de-duplication,
    # otherwise modules could not be re-run in the space of 5 Minutes.
    sqs.send_message(QueueUrl=queue_url, MessageBody=message,
                        MessageGroupId=message_group_id, MessageDeduplicationId=message_dedup_id)

    
def lambda_handler(event, context):
    try:
        
        # ENV vars
        error_handler_arn = os.environ['error_handler_arn']
        queue_url = os.environ['queue_url']
        checkpoint = os.environ['checkpoint']
        function_name = os.environ['function_name']
        questions_list = os.environ['questions_list']
        sqs_messageid_name = os.environ['sqs_messageid_name']

        message_json, receipt_handle = _get_sqs_message()
        
        data = pd.DataFrame(message_json)
         
        # Add means columns
        for question in questions_list:
                data['mean_' + question] = 0.0
                
        data_json = data.to_json(orient='records')

        returned_data = lambda_client.invoke(FunctionName=function_name, Payload=json.dumps(message_json))
        json_response = returned_data.get('Payload').read().decode("UTF-8")
        
        random_id = str(random.getrandbits(128))
        send_sqs_message(queue_url, json_response, sqs_messageid_name, random_id)
        
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        
        # imputation_run_type = "Calculate Means was run successfully."

        # send_sns_message(arn, imputation_run_type, "No anomalies.")
        
        # Currently due to POC Code if Imputation is performed just imputed data is sent onwards
        final_output = json_response
        
    except Exception as exc:
        ### COMMENTED OUT FOR TESTING ###
        # purge = sqs.purge_queue(
        #     QueueUrl=queue_url
        # )

        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return {
        "success": True,
        "checkpoint": checkpoint
    }
