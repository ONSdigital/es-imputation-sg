import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import Schema, fields


class EnvironmentSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    response_type = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the calculate movements statistical method.
    The method requires a column per question to store the movements, named as follows:
    'movement_questionNameAndNumber'. The wrangler checks for non response and if everyone
    has responded the calculate movements is skipped.
    :param event: Contains Runtime_variables, which contains the movement_type
    :param context: N/A
    :return: Success & Checkpoint & Impute/Error - Type: JSON
    """
    to_be_imputed = True
    current_module = "Imputation Movement - Wrangler."
    logger = logging.getLogger(current_module)
    logger.setLevel(10)
    error_message = ""
    checkpoint = 0
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Set up clients
        sqs = boto3.client("sqs", region_name="eu-west-2")
        lambda_client = boto3.client("lambda", region_name="eu-west-2")
        logger.info("Setting-up environment configs")

        schema = EnvironmentSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")
        logger.info("Validated params")

        # Environment Variables
        bucket_name = config["bucket_name"]
        checkpoint = config["checkpoint"]
        method_name = config["method_name"]
        response_type = config["response_type"]

        # Runtime Variables
        current_data = event["RuntimeVariables"]["current_data"]
        in_file_name = event["RuntimeVariables"]["in_file_name"]
        incoming_message_group_id = event["RuntimeVariables"]["incoming_message_group_id"]
        location = event["RuntimeVariables"]["location"]
        movement_type = event["RuntimeVariables"]["movement_type"]
        out_file_name = event["RuntimeVariables"]["out_file_name"]
        out_file_name_skip = event["RuntimeVariables"]["out_file_name_skip"]
        outgoing_message_group_id = event["RuntimeVariables"]["outgoing_message_group_id"]
        outgoing_message_group_id_skip = event["RuntimeVariables"][
            "outgoing_message_group_id_skip"]
        period = event["RuntimeVariables"]["period"]
        period_column = event["RuntimeVariables"]["period_column"]
        periodicity = event["RuntimeVariables"]["periodicity"]
        previous_data = event["RuntimeVariables"]["previous_data"]
        questions_list = event["RuntimeVariables"]["questions_list"]
        reference = event["RuntimeVariables"]["unique_identifier"][0]
        sns_topic_arn = event["RuntimeVariables"]["sns_topic_arn"]
        sqs_queue_url = event["RuntimeVariables"]["queue_url"]

        logger.info("Retrieved configuration variables.")

        data, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                            in_file_name,
                                                            incoming_message_group_id,
                                                            location)

        previous_period = general_functions.calculate_adjacent_periods(period,
                                                                       periodicity)
        logger.info("Completed reading data from s3")
        previous_period_data = data[
            data[period_column].astype("str") == str(previous_period)]
        data = data[
            data[period_column].astype("str") == str(period)]
        logger.info("Split input data")

        # Create a Dataframe where the response column
        # value is set as 1 i.e non responders
        filtered_non_responders = data.loc[(data[response_type] == 1) &
                                           (data[period_column].astype("str") ==
                                            str(period))]

        logger.info("Successfully created filtered non responders DataFrame")

        response_check = len(filtered_non_responders.index)

        # If greater than 0 it means there is non-responders so Imputation need to be run
        if response_check > 0:

            # Save previous period data to s3 for apply to pick up later
            aws_functions.save_to_s3(bucket_name, previous_data,
                                     previous_period_data.to_json(orient="records"),
                                     location)
            # Save raw data to s3 for apply to pick up later
            aws_functions.save_to_s3(bucket_name, current_data,
                                     data.to_json(orient="records"), location)
            logger.info("Successfully sent data.")

            # Ensure that only responder_ids with a response
            # type of 2 (returned) get picked up
            data = data[data[response_type] == 2]
            previous_period_data = \
                previous_period_data[previous_period_data[response_type] == 2]

            # Ensure that only rows that exist in both current and previous get picked up.
            data = data[data[reference].isin(previous_period_data[reference])].dropna()
            previous_period_data = previous_period_data[previous_period_data[reference].isin(data[reference])].dropna()  # noqa e501

            # Merged together so it can be sent via the payload to the method
            merged_data = pd.concat([data, previous_period_data])

            logger.info("Successfully filtered and merged the previous period data")

            for question in questions_list:
                merged_data["movement_" + question] = 0.0

            json_ordered_data = merged_data.to_json(orient="records")

            json_payload = {
                "RuntimeVariables": {
                    "data": json.loads(json_ordered_data),
                    "movement_type": movement_type,
                    "questions_list": questions_list,
                    "current_period": period,
                    "period_column": period_column,
                    "previous_period": previous_period,
                    "run_id": run_id
                }
            }

            logger.info("Successfully created movement columns on the data")

            imputed_data = lambda_client.invoke(FunctionName=method_name,
                                                Payload=json.dumps(json_payload))

            logger.info("Successfully invoked method.")

            json_response = json.loads(imputed_data.get("Payload").read().decode("UTF-8"))
            logger.info("JSON extracted from method response.")

            if not json_response["success"]:
                raise exception_classes.MethodFailure(json_response["error"])

            imputation_run_type = "Calculate Movement."
            aws_functions.save_data(bucket_name, out_file_name,
                                    json_response["data"], sqs_queue_url,
                                    outgoing_message_group_id, location)

            logger.info("Successfully sent the data to s3")

        else:

            to_be_imputed = False
            imputation_run_type = "Has Not Run."

            aws_functions.save_data(
                bucket_name,
                out_file_name_skip,
                data.to_json(orient="records"),
                sqs_queue_url,
                outgoing_message_group_id_skip,
                location
            )

            logger.info("Successfully sent the unchanged data to s3")

            if receipt_handler:
                sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

            aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                           "Imputation - Did not run")
            logger.info("Successfully sent message to sns")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        aws_functions.send_sns_message(
            checkpoint, sns_topic_arn,
            "Imputation - " + imputation_run_type)

        logger.info("Successfully sent the SNS message")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {
        "success": True,
        "checkpoint": checkpoint,
        "impute": to_be_imputed
    }
