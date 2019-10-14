import json
import logging
import os
import random

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError

from marshmallow import Schema, fields


class EnvironSchema(Schema):
    arn = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    questions = fields.Str(required=True)


class NoDataInQueueError(Exception):
    pass


def lambda_handler(event, context):
    """
    Prepares data for and calls the Calculate imputation factors method.
    - adds on the required columns needed by the method.

    :param event: lambda event
    :param context: lambda context
    :return: string
    """
    current_module = "Calculate Factors - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("CalculateFactors")
    logger.setLevel(10)
    try:
        logger.info("Calculate Factors Wrangler Begun")
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

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
        if "Messages" not in response:
            raise NoDataInQueueError("No Messages in queue")

        message = response["Messages"][0]
        message_json = json.loads(message["Body"])

        # reciept handler used to clear sqs queue
        receipt_handle = message["ReceiptHandle"]

        logger.info("Successfully retrieved data from sqs")

        data = pd.DataFrame(message_json)

        # create df columns needed for method
        for question in questions.split(" "):
            data["imputation_factor_" + question] = 0

        data_json = data.to_json(orient="records")

        logger.info("Successfully wrangled data from sqs")
        # invoke the method to calculate the factors
        calculate_factors = lambda_client.invoke(
            FunctionName=method_name, Payload=data_json
        )
        json_response = calculate_factors.get("Payload").read().decode("UTF-8")

        logger.info("Successfully invoked lambda")

        # send data to sqs queue
        send_sqs_message(queue_url, json.loads(json_response), sqs_messageid_name)
        logger.info("Successfully sent data to sqs")

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Successfully deleted input data from sqs")

        send_sns_message(arn, "Imputation Factors Calculated", checkpoint)
        logger.info("Successfully sent data to sns")
    except NoDataInQueueError as e:
        error_message = (
            "There was no data in sqs queue in:  "
            + current_module
            + " |-  | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Parameter validation error"
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ClientError as e:
        error_message = (
            "AWS Error ("
            + str(e.response["Error"]["Code"])
            + ") "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
            "Incomplete Lambda response encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = (
            "General Error in "
            + current_module
            + " ("
            + str(type(e))
            + ") |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
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

    return sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message,
        MessageGroupId=output_message_id,
        MessageDeduplicationId=str(random.getrandbits(128)),
    )
