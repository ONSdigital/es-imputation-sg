import json
import logging
import os

import boto3
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields

import imputation_functions as imp_func


class InputSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    The wrangler is responsible for preparing the data so the IQRS method can be applied.
    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Success & Checkpoint/Error - Type: JSON
    """
    current_module = "Imputation IQRS - Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("IQRS")
    logger.setLevel(10)
    # Define run_id outside of try block
    run_id = 0
    try:

        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        # clients
        sqs = boto3.client('sqs', region_name="eu-west-2")
        lambda_client = boto3.client('lambda', region_name="eu-west-2")

        # env vars
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")
        logger.info("Validated params")

        # Environment variables
        bucket_name = config['bucket_name']
        checkpoint = config['checkpoint']
        method_name = config['method_name']
        run_environment = config['run_environment']
        sns_topic_arn = config['sns_topic_arn']

        # Runtime Variables
        distinct_values = event['RuntimeVariables']["distinct_values"]
        in_file_name = event['RuntimeVariables']['in_file_name']
        incoming_message_group_id = event['RuntimeVariables']['incoming_message_group_id']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        questions_list = event['RuntimeVariables']['questions_list']
        sqs_queue_url = event['RuntimeVariables']["queue_url"]

        logger.info("Retrieved configuration variables.")

        data, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                            in_file_name,
                                                            incoming_message_group_id,
                                                            run_id)
        logger.info("Successfully retrieved data.")
        iqrs_columns = imp_func.produce_columns("iqrs_", questions_list)
        for col in iqrs_columns:
            data[col] = 0

        logger.info("IQRS columns successfully added")

        data_json = data.to_json(orient='records')

        logger.info("Dataframe converted to JSON")

        payload = {"data": json.loads(data_json),
                   "distinct_values": distinct_values,
                   "questions_list": questions_list}

        wrangled_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload)
        )
        logger.info("Successfully invoked method.")

        json_response = json.loads(wrangled_data.get('Payload').read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        aws_functions.save_data(bucket_name, out_file_name,
                                json_response["data"], sqs_queue_url,
                                outgoing_message_group_id, run_id)
        logger.info("Successfully sent data to s3.")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)
            logger.info("Successfully deleted message from sqs.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, in_file_name, run_id))
            logger.info("Successfully deleted input data from s3.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn, 'Imputation - IQRs.')
        logger.info("Successfully sent message to sns.")

    except AttributeError as e:
        error_message = (
            "Bad data encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
            + " | Run_id: " + str(run_id)
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
            + " | Run_id: " + str(run_id)
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
            + " | Run_id: " + str(run_id)
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
            + " | Run_id: " + str(run_id)
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
            + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "." + " | Run_id: " + str(run_id)
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
            + " | Run_id: " + str(run_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
