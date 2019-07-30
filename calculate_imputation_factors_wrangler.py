import json
import traceback
import random
import os
import pandas as pd
import boto3
from marshmallow import Schema, fields


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.

    :param exception: Exception object
    :return: string
    """

    return "".join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


class EnvironSchema(Schema):
    arn = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    questions = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Prepares data for and calls the Calculate imputation factors method.
    - adds on the required columns needed by the method.

    :param event: lambda event
    :param context: lambda context
    :return: string
    """
    try:
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # environment variables
        sqs = boto3.client("sqs")
        lambda_client = boto3.client("lambda")

        queue_url = config["queue_url"]
        sqs_messageid_name = config["sqs_messageid_name"]

        method_name = config["method_name"]
        questions = config["questions"]
        checkpoint = config["checkpoint"]
        arn = config["arn"]
        # read in data from the sqs queue
        response = sqs.receive_message(QueueUrl=queue_url)
        message = response["Messages"][0]
        message_json = json.loads(message["Body"])

        # reciept handler used to clear sqs queue
        receipt_handle = message["ReceiptHandle"]

        data = pd.DataFrame(message_json)

        # create df columns needed for method
        for question in questions.split(" "):
            data["imputation_factor_" + question] = 0

        data_json = data.to_json(orient="records")

        # invoke the method to calculate the factors
        calculate_factors = lambda_client.invoke(
            FunctionName=method_name, Payload=data_json
        )
        json_response = calculate_factors.get("Payload").read().decode("UTF-8")

        # send data to sqs queue
        send_sqs_message(queue_url, json.loads(json_response), sqs_messageid_name)

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        send_sns_message(arn, "Imputation Factors Calculated", checkpoint)
    except Exception as exc:
        print(_get_traceback(exc))
        checkpoint = config["checkpoint"]
        return {
            "success": False,
            "checkpoint": checkpoint,
            "error": "Unexpected exception {}".format(_get_traceback(exc)),
        }

    return {"success": True, "checkpoint": checkpoint}


def send_sns_message(arn, imputation_run_type, checkpoint):
    """
    This function is responsible for sending notifications to the SNS Topic.
    :param arn: The address of the sns topic - String
    :param imputation_run_type: A message indicating
            where in the process we are. - String
    :param checkpoint: Current location in imputation process.
    :return:
    """

    sns = boto3.client("sns")

    sns_message = {
        "success": True,
        "module": "Imputation Calculate Imputation Factors",
        "checkpoint": checkpoint,
        "message": imputation_run_type,
    }

    sns.publish(TargetArn=arn, Message=json.dumps(sns_message))


def send_sqs_message(queue_url, message, output_message_id):
    """
    This method is responsible for sending data to the SQS queue.

    :param queue_url: The url of the SQS queue. - Type: String.
    :param message: The message/data you wish to send to the SQS queue - Type: String.
    :param output_message_id: The label of the record in the SQS queue - Type: String
    :return: None
    """
    sqs = boto3.client("sqs")

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageGroupId=output_message_id,
        MessageDeduplicationId=str(random.getrandbits(128)),
    )
