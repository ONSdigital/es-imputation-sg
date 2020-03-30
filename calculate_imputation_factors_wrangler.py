import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import Schema, fields

import imputation_functions as imp_func


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)


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

    logger = logging.getLogger("CalculateFactors")

    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting " + current_module)

        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        sqs = boto3.client("sqs")
        lambda_client = boto3.client("lambda")

        # Environment variables
        bucket_name = config['bucket_name']
        checkpoint = config["checkpoint"]
        method_name = config["method_name"]
        run_environment = config['run_environment']

        # Runtime Variables
        distinct_values = event['RuntimeVariables']["distinct_values"]
        factors_parameters = event['RuntimeVariables']["factors_parameters"]
        in_file_name = event['RuntimeVariables']['in_file_name']
        incoming_message_group_id = event['RuntimeVariables']['incoming_message_group_id']
        location = event['RuntimeVariables']['location']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        period_column = event['RuntimeVariables']["period_column"]
        questions_list = event['RuntimeVariables']['questions_list']
        sns_topic_arn = event['RuntimeVariables']["sns_topic_arn"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]

        logger.info("Retrieved configuration variables.")

        data, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                            in_file_name,
                                                            incoming_message_group_id,
                                                            location)

        logger.info("Successfully retrieved data")

        factor_columns = imp_func.\
            produce_columns("imputation_factor_", questions_list)

        # create df columns needed for method
        for factor in factor_columns:
            data[factor] = 0

        logger.info("Successfully wrangled data from sqs")

        payload = {
            "RuntimeVariables": {
                "data_json": json.loads(data.to_json(orient="records")),
                "questions_list": questions_list,
                "distinct_values": distinct_values,
                "factors_parameters": factors_parameters,
                "run_id": run_id
            }
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
                                             questions_list,
                                             distinct_values
                                            )

        final_df = output_df[columns_to_keep].drop_duplicates().to_json(orient='records')
        aws_functions.save_data(bucket_name, out_file_name,
                                final_df, sqs_queue_url,
                                outgoing_message_group_id, location)
        logger.info("Successfully sent data to s3.")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)
            logger.info("Successfully deleted message from sqs.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, in_file_name, location))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                       "Imputation - Calculate Factors.")
        logger.info("Successfully sent message to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {"success": True, "checkpoint": checkpoint}
