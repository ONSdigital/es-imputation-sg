import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    arn = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    questions = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    bucket_name = fields.Str(required=True)


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
    try:
        logger.info("Calculate Factors Wrangler Begun")
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        sqs = boto3.client("sqs")
        lambda_client = boto3.client("lambda")
        # environment variables
        queue_url = config["queue_url"]
        sqs_messageid_name = config["sqs_messageid_name"]

        method_name = config["method_name"]
        questions = config["questions"]
        checkpoint = config["checkpoint"]
        arn = config["arn"]
        incoming_message_group = config['incoming_message_group']
        in_file_name = config["in_file_name"]
        out_file_name = config["out_file_name"]
        bucket_name = config['bucket_name']

        data, receipt_handle = funk.get_dataframe(queue_url, bucket_name,
                                                  in_file_name,
                                                  incoming_message_group)

        logger.info("Successfully retrieved data")

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

        funk.save_data(bucket_name, out_file_name,
                       json_response, queue_url, sqs_messageid_name)

        logger.info("Successfully sent data to sqs")

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Successfully deleted input data from sqs")

        logger.info(funk.delete_data(bucket_name, in_file_name))

        imputation_run_type = "Imputation Factors Calculated successfully."

        funk.send_sns_message(checkpoint, arn, imputation_run_type)
        logger.info("Successfully sent data to sns")

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