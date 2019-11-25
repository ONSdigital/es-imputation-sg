import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields

import imputation_functions as imp_func


class InputSchema(Schema):
    checkpoint = fields.Str(required=True)
    question_list = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)


def lambda_handler(event, context):
    current_module = "Imputation Atypicals - Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Atypicals")
    logger.setLevel(10)
    try:

        logger.info("Starting " + current_module)

        # Environment Variables.
        sqs = boto3.client('sqs', region_name="eu-west-2")
        lambda_client = boto3.client('lambda', region_name="eu-west-2")
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        checkpoint = config["checkpoint"]
        question_list = config["question_list"]

        bucket_name = config["bucket_name"]
        in_file_name = config["in_file_name"]
        incoming_message_group = config["incoming_message_group"]
        method_name = config["method_name"]
        out_file_name = config["out_file_name"]
        sns_topic_arn = config["sns_topic_arn"]
        sqs_message_group_id = config["sqs_message_group_id"]
        sqs_queue_url = config["sqs_queue_url"]

        logger.info("Vaildated params")

        data, receipt_handler = funk.get_dataframe(sqs_queue_url, bucket_name,
                                                   in_file_name,
                                                   incoming_message_group)

        logger.info("Succesfully retrieved data.")
        atypical_columns = imp_func.produce_columns("atyp_", question_list.split(','), [])

        for col in atypical_columns:
            data[col] = 0

        logger.info("Atypicals columns succesfully added")

        data_json = data.to_json(orient='records')

        payload = {
            "json_data": json.loads(data_json),
            "question_list": question_list
        }

        logger.info("Dataframe converted to JSON")

        wrangled_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload)
        )

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")

        logger.info("Succesfully invoked method lambda")

        funk.save_data(bucket_name, out_file_name,
                       json_response, sqs_queue_url, sqs_message_group_id)
        logger.info("Successfully sent data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        logger.info(funk.delete_data(bucket_name, in_file_name))
        logger.info("Successfully deleted input data.")

        logger.info(funk.send_sns_message(checkpoint, sns_topic_arn,
                                          "Imputation - Atypicals."))

        logger.info("Succesfully sent message to sns")

    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
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
