import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    response_type = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    current_data = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    movement_type = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    out_file_name_skip = fields.Str(required=True)
    period = fields.Str(required=True)
    period_column = fields.Str(required=True)
    periodicity = fields.Str(required=True)
    previous_data = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    sns_topic_arn = fields.Str(required=True)
    unique_identifier = fields.List(fields.String, required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the calculate movements statistical method.
    The method requires a column per question to store the movements, named as follows:
    'movement_questionNameAndNumber'. The wrangler checks for non response and if everyone
    has responded the calculate movements is skipped.
    :param event: Contains Runtime_variables, which contains the movement_type
    :param context: N/A
    :return: Success & Impute/Error - Type: JSON
    """
    to_be_imputed = True
    current_module = "Imputation Movement - Wrangler."
    logger = logging.getLogger(current_module)
    logger.setLevel(10)
    error_message = ""
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Set up clients
        lambda_client = boto3.client("lambda", region_name="eu-west-2")
        logger.info("Setting-up environment configs")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        response_type = environment_variables["response_type"]

        # Runtime Variables
        current_data = runtime_variables["current_data"]
        in_file_name = runtime_variables["in_file_name"]
        movement_type = runtime_variables["movement_type"]
        out_file_name = runtime_variables["out_file_name"]
        out_file_name_skip = runtime_variables["out_file_name_skip"]
        period = runtime_variables["period"]
        period_column = runtime_variables["period_column"]
        periodicity = runtime_variables["periodicity"]
        previous_data = runtime_variables["previous_data"]
        questions_list = runtime_variables["questions_list"]
        reference = runtime_variables["unique_identifier"][0]
        sns_topic_arn = runtime_variables["sns_topic_arn"]

        logger.info("Retrieved configuration variables.")

        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

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
                                     previous_period_data.to_json(orient="records"))
            # Save raw data to s3 for apply to pick up later
            aws_functions.save_to_s3(bucket_name, current_data,
                                     data.to_json(orient="records"))
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

            # Make sure there is some data, non-responders were removed at this stage
            if len(merged_data.index) > 0:
                logger.info("Successfully filtered and merged the previous period data")
            else:
                raise exception_classes.LambdaFailure("No data left after filtering")

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
            aws_functions.save_to_s3(bucket_name, out_file_name, json_response["data"])

            logger.info("Successfully sent the data to s3")

        else:

            to_be_imputed = False
            imputation_run_type = "Has Not Run."

            aws_functions.save_to_s3(bucket_name, out_file_name_skip,
                                     data.to_json(orient="records"))

            logger.info("Successfully sent the unchanged data to s3")

            aws_functions.send_sns_message(sns_topic_arn, "Imputation - Did not run")
            logger.info("Successfully sent message to sns")

        aws_functions.send_sns_message(sns_topic_arn,
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
        "impute": to_be_imputed
    }
