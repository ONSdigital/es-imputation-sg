import json
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
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    questions_list = fields.Str(required=True)


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
    current_module = "Imputation Recalculate Means - Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("RecalcMeans")
    logger.setLevel(10)
    try:
        logger.info("Starting " + current_module)
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

        distinct_values = event['RuntimeVariables']["distinct_values"].split(",")

        data, receipt_handler = funk.get_dataframe(
            sqs_queue_url,
            bucket_name,
            in_file_name,
            incoming_message_group
        )

        logger.info("Successfully retrieved data")

        # Add means columns
        for question in questions_list.split(','):
            data.drop(['movement_' + question + '_count'], axis=1, inplace=True)
            data.drop(['movement_' + question + '_sum'], axis=1, inplace=True)
            data.drop(['atyp_' + question, 'iqrs_' + question], axis=1, inplace=True)
            data['mean_' + question] = 0.0

        data_json = data.to_json(orient='records')

        payload = {
            "json_data": json.loads(data_json),
            "distinct_values": distinct_values,
            "questions_list": questions_list
        }

        returned_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload)
        )

        json_response = json.loads(returned_data.get('Payload').read().decode("UTF-8"))
        if str(type(json_response)) != "<class 'str'>":
            raise funk.MethodFailure(json_response['error'])
        logger.info("Successfully invoked lambda")

        funk.save_data(bucket_name, out_file_name,
                       json_response, sqs_queue_url, sqs_message_group_id)

        logger.info("Successfully sent data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        logger.info("Successfully deleted message from sqs")

        logger.info(funk.delete_data(bucket_name, in_file_name))

        funk.send_sns_message(checkpoint, sns_topic_arn,
                              'Imputation - Recalculate Means.')
        logger.info("Successfully sent message to sns")

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
