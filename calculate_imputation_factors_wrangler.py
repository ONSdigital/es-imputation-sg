import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Prepares data for and calls the Calculate imputation factors method by adding on the
    required columns needed by the method.
    :param event: Contains all the variables which are required for the specific run.
    :param context: lambda context
    :return: Success & Checkpoint/Error - Type: JSON
    """
    current_module = "Imputation Calculate Factors - Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("CalculateFactors")
    try:
        logger.info("Starting " + current_module)
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        sqs = boto3.client("sqs")
        lambda_client = boto3.client("lambda")

        # Environment variables
        checkpoint = config["checkpoint"]
        bucket_name = config['bucket_name']
        in_file_name = config["in_file_name"]
        incoming_message_group = config['incoming_message_group']
        method_name = config["method_name"]
        out_file_name = config["out_file_name"]
        questions_list = config["questions_list"]
        sns_topic_arn = config["sns_topic_arn"]
        sqs_message_group_id = config["sqs_message_group_id"]
        sqs_queue_url = config["sqs_queue_url"]

        data, receipt_handler = funk.get_dataframe(sqs_queue_url, bucket_name,
                                                   in_file_name,
                                                   incoming_message_group)

        logger.info("Successfully retrieved data")

        # create df columns needed for method
        for question in questions_list.split(","):
            data["imputation_factor_" + question] = 0

        logger.info("Successfully wrangled data from sqs")

        payload = {
            "data_json": data.to_json(orient="records"),
            "questions_list": questions_list
        }

        # invoke the method to calculate the factors
        calculate_factors = lambda_client.invoke(
            FunctionName=method_name, Payload=payload
        )
        json_response = json.loads(
            calculate_factors.get("Payload").read().decode("UTF-8"))

        logger.info("Successfully invoked lambda")

        funk.save_data(bucket_name, out_file_name,
                       json_response, sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent data to sqs")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        logger.info("Successfully deleted input data from sqs")

        logger.info(funk.delete_data(bucket_name, in_file_name))

        funk.send_sns_message(checkpoint, sns_topic_arn,
                              "Imputation - Calculate Factors.")
        logger.info("Successfully sent message to sns")

    except ValueError as e:
        error_message = (
            "Parameter validation error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
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
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
            "Incomplete Lambda response encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
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
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
