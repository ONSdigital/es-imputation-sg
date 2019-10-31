import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Class to set up the environment variables schema.
    """
    arn = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    file_name = fields.Str(required=True)
    bucket_name = fields.Str(required=True)


def lambda_handler(event, context):
    """
    prepares the data for the Means method.
    - Read in data from the SQS queue.
    - Invokes the Mean Method.
    - Send data received back from the Mean method to the SQS queue.

    :param event:
    :param context:
    :return: Outcome Message - Type: Json String.
    """
    current_module = "Recalc - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("RecalcMeans")
    logger.setLevel(10)
    try:
        logger.info(current_module + " Begun")
        # Set up clients
        sqs = boto3.client('sqs', 'eu-west-2')
        lambda_client = boto3.client('lambda', 'eu-west-2')

        # Set up Environment variables Schema.
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        logger.info("Validated params")
        # Set up environment variables
        arn = config["arn"]
        queue_url = config["queue_url"]
        checkpoint = config["checkpoint"]
        method_name = config["method_name"]
        questions_list = config["questions_list"]
        sqs_messageid_name = config["sqs_messageid_name"]
        file_name = config['file_name']
        incoming_message_group = config['incoming_message_group']
        bucket_name = config['bucket_name']

        data, receipt_handle = funk.get_dataframe(queue_url, bucket_name,
                                                  "atypicals_out.json",
                                                  incoming_message_group)

        logger.info("Successfully retrieved data")

        # Add means columns
        qno = 1
        for question in questions_list.split(' '):
            data.drop(['movement_' + question + '_count'], axis=1, inplace=True)
            data.drop(['movement_' + question + '_sum'], axis=1, inplace=True)
            data.drop(['atyp60' + str(qno), "iqrs60" + str(qno)], axis=1, inplace=True)
            qno += 1
            data['mean_' + question] = 0.0

        data_json = data.to_json(orient='records')

        returned_data = lambda_client.invoke(FunctionName=method_name, Payload=data_json)

        json_response = returned_data.get('Payload').read().decode("UTF-8")

        logger.info("Successfully invoked lambda")

        funk.save_data(bucket_name, file_name,
                       json_response, queue_url, sqs_messageid_name)

        logger.info("Successfully sent data to sqs")

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        logger.info("Successfully deleted input data from sqs")

        logger.info(funk.delete_data(bucket_name, "atypicals_out.json"))

        imputation_run_type = "Recalculate Means was run successfully."

        funk.send_sns_message(checkpoint, arn, imputation_run_type)
        logger.info("Successfully sent data to sns")

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
