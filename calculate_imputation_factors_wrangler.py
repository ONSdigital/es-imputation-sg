import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields

import imputation_functions as imp_func


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
        
        distinct_values = event['RuntimeVariables']["distinct_values"]
        period_column = event['RuntimeVariables']["period_column"]
        factors_parameters = event['RuntimeVariables']["factors_parameters"]

        data, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                            in_file_name,
                                                            incoming_message_group)

        logger.info("Successfully retrieved data")

        factor_columns = imp_func.\
            produce_columns("imputation_factor_", questions_list.split(','))

        # create df columns needed for method
        for factor in factor_columns:
            data[factor] = 0

        logger.info("Successfully wrangled data from sqs")

        payload = {
            "data_json": json.loads(data.to_json(orient="records")),
            "questions_list": questions_list,
            "distinct_values": distinct_values,
            "factors_parameters": factors_parameters
        }

        # invoke the method to calculate the factors
        calculate_factors = lambda_client.invoke(
            FunctionName=method_name, Payload=json.dumps(payload)
        )
        logger.info("Successfully invoked method.")

        json_response = json.loads(
            calculate_factors.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        output_df = pd.read_json(json_response['data'], dtype=False)
        distinct_values.append(period_column)
        columns_to_keep = imp_func.produce_columns(
                                             "imputation_factor_",
                                             questions_list.split(','),
                                             distinct_values
                                            )

        final_df = output_df[columns_to_keep].drop_duplicates().to_json(orient='records')
        aws_functions.save_data(bucket_name, out_file_name,
                                final_df, sqs_queue_url,
                                sqs_message_group_id)

        logger.info("Successfully sent data to sqs")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        logger.info("Successfully deleted input data from sqs")

        logger.info(aws_functions.delete_data(bucket_name, in_file_name))

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
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
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."
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

    logger.info("Successfully completed module: " + current_module)
    return {"success": True, "checkpoint": checkpoint}
