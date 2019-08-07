import json
import os
import random
import traceback

import boto3
import marshmallow
import pandas as pd


class InputSchema(marshmallow.Schema):
    queue_url = marshmallow.fields.Str(required=True)
    checkpoint = marshmallow.fields.Str(required=True)
    function_name = marshmallow.fields.Str(required=True)
    questions_list = marshmallow.fields.Str(required=True)
    sqs_messageid_name = marshmallow.fields.Str(required=True)
    arn = marshmallow.fields.Str(required=True)


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


def lambda_handler(event, context):
    # Set up clients
    sqs = boto3.client("sqs")
    lambda_client = boto3.client("lambda")
    sns = boto3.client("sns")

    # ENV vars
    config, errors = InputSchema().load(os.environ)
    if errors:
        raise ValueError(f"Error validating environment params: {errors}")

    try:

        # Reads in Data from SQS Queue
        response = sqs.receive_message(QueueUrl=config['queue_url'])
        message = response["Messages"][0]
        message_json = json.loads(message["Body"])

        receipt_handle = message["ReceiptHandle"]

        data = pd.DataFrame(message_json)

        # Add means columns
        for question in config['questions_list'].split(" "):
            data[question] = 0.0

        data_json = data.to_json(orient="records")

        returned_data = lambda_client.invoke(
            FunctionName=config['function_name'], Payload=data_json
        )
        json_response = returned_data.get("Payload").read().decode("UTF-8")

        # MessageDeduplicationId is set to a random hash to overcome de-duplication,
        # otherwise modules could not be re-run in the space of 5 Minutes
        sqs.send_message(
            QueueUrl=config['queue_url'],
            MessageBody=json.loads(json_response),
            MessageGroupId=config['sqs_messageid_name'],
            MessageDeduplicationId=str(random.getrandbits(128)),
        )

        sqs.delete_message(QueueUrl=config['queue_url'], ReceiptHandle=receipt_handle)

        imputation_run_type = "Calculate Means was run successfully."

        send_sns_message(imputation_run_type, config['checkpoint'], sns, config['arn'])

        # Currently due to POC Code if Imputation is performed,
        # just imputed data is sent onwards.
        # final_output = json_response

    except Exception as exc:
        return {
            "success": False,
            "checkpoint": config['checkpoint'],
            "error": "Unexpected exception {}".format(_get_traceback(exc)),
        }

    return {"success": True, "checkpoint": config['checkpoint']}


def send_sns_message(imputation_run_type, checkpoint, sns, arn):
    sns_message = {
        "success": True,
        "module": "Imputation",
        "checkpoint": checkpoint,
        "message": imputation_run_type,
    }

    sns.publish(TargetArn=arn, Message=json.dumps(sns_message))
