import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields

import imputation_functions as imp_func


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    distinct_values = fields.List(fields.String, required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    The wrangler ingests data from the movements step, checks for anomalies (TBC), and
    formats the data to be passed through to the method.
    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Success & None/Error - Type: JSON
    """
    current_module = "Imputation Means - Wrangler."
    error_message = ""
    logger = general_functions.get_logger()

    # Define run_id outside of try block
    run_id = 0

    # Set-up variables for status message
    bpm_queue_url = None

    try:

        logger.info("Starting " + current_module)

        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Set up clients
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        distinct_values = runtime_variables["distinct_values"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        questions_list = runtime_variables["questions_list"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]

        logger.info("Retrieved configuration variables.")

        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Successfully retrieved data")

        means_columns = imp_func.produce_columns("mean_", questions_list)
        for question in means_columns:
            data[question] = 0.0

        logger.info("Means columns successfully added")

        data_json = data.to_json(orient="records")

        logger.info("Dataframe converted to JSON")

        payload = {
            "RuntimeVariables": {
                "bpm_queue_url": bpm_queue_url,
                "data": json.loads(data_json),
                "distinct_values": distinct_values,
                "questions_list": questions_list,
                "run_id": run_id
            }
        }

        returned_data = lambda_client.invoke(
            FunctionName=method_name, Payload=json.dumps(payload)
        )
        logger.info("Successfully invoked method.")

        json_response = json.loads(returned_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        aws_functions.save_to_s3(bucket_name, out_file_name, json_response["data"])
        logger.info("Successfully sent data to s3.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, in_file_name))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(sns_topic_arn, "Imputation - Calculate Means.")
        logger.info("Successfully sent message to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)
    return {"success": True}
