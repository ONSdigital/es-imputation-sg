import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class InputSchema(Schema):
    bucket_name = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    arn = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    atypical_columns = fields.Str(required=True)
    method_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)


def lambda_handler(event, context):
    current_module = "Atypicals - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Atypicals")
    logger.setLevel(10)
    try:

        logger.info("Atypicals Wrangler Begun")

        # Environment Variables.
        sqs = boto3.client('sqs', region_name="eu-west-2")
        lambda_client = boto3.client('lambda', region_name="eu-west-2")
        sns = boto3.client('sns', region_name="eu-west-2")
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")
        bucket_name = config["bucket_name"]
        queue_url = config["queue_url"]
        sqs_messageid_name = config["sqs_messageid_name"]
        method_name = config["method_name"]
        atypical_columns = config["atypical_columns"]
        checkpoint = config["checkpoint"]
        arn = config["arn"]
        incoming_message_group = config["incoming_message_group"]
        in_file_name = config["in_file_name"]
        out_file_name = config["out_file_name"]

        logger.info("Vaildated params")

        data, receipt_handler = funk.get_dataframe(queue_url, bucket_name,
                                                   in_file_name,
                                                   incoming_message_group)

        logger.info("Succesfully retrieved data.")

        for col in atypical_columns.split(','):
            data[col] = 0

        logger.info("Atypicals columns succesfully added")

        data_json = data.to_json(orient='records')

        logger.info("Dataframe converted to JSON")

        wrangled_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(data_json)
        )

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")

        logger.info("Succesfully invoked method lambda")

        funk.save_data(bucket_name, out_file_name,
                       json_response, queue_url, sqs_messageid_name)
        logger.info("Successfully sent data to sqs")

        if receipt_handler:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handler)

        logger.info(funk.delete_data(bucket_name, in_file_name))
        logger.info("Successfully deleted input data.")

        logger.info(funk.send_sns_message(checkpoint, sns, arn))

        logger.info("Succesfully sent data to sns")

    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
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
