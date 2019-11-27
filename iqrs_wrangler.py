import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields

from imputation_functions import produce_columns


class InputSchema(Schema):
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
    The wrangler is responsible for preparing the data so the IQRS method can be applied.
    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Returns the Input data with the IQRS added onto the Data set.
    """
    current_module = "Imputation IQRS - Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("IQRS")
    logger.setLevel(10)
    try:

        logger.info("Starting " + current_module)

        # clients
        sqs = boto3.client('sqs', region_name="eu-west-2")
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # env vars
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        in_file_name = config["in_file_name"]
        incoming_message_group = config['incoming_message_group']
        method_name = config['method_name']
        out_file_name = config["out_file_name"]
        questions_list = config['questions_list']
        sns_topic_arn = config['sns_topic_arn']
        sqs_queue_url = config['sqs_queue_url']
        sqs_message_group_id = config['sqs_message_group_id']

        distinct_values = event['RuntimeVariables']["distinct_values"]

        logger.info("Vaildated params")

        data, receipt_handler = funk.get_dataframe(sqs_queue_url, bucket_name,
                                                   in_file_name,
                                                   incoming_message_group)
        logger.info("Succesfully retrieved data.")

        for col in produce_columns("iqrs_", questions_list.split(',')):
            data[col] = 0

        logger.info("IQRS columns succesfully added")

        data_json = data.to_json(orient='records')

        logger.info("Dataframe converted to JSON")

        payload = {"data": json.loads(data_json),
                   "distinct_values": distinct_values,
                   "questions_list": questions_list}

        wrangled_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload)
        )

        json_response = wrangled_data.get('Payload').read().decode("UTF-8")

        if str(type(json_response)) != "<class 'str'>":
            raise funk.MethodFailure(json_response['error'])
        logger.info("Succesfully invoked method lambda")

        funk.save_data(bucket_name, out_file_name,
                       json_response, sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        logger.info(funk.delete_data(bucket_name, in_file_name))
        logger.info("Successfully deleted input data from s3")
        funk.send_sns_message(checkpoint, sns_topic_arn, 'Imputation - IQRs.')

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
    except funk.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
