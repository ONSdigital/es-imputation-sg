import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields
from marshmallow.validate import Equal


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)


class FactorsSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    region_column = fields.Str(required=True)
    regionless_code = fields.Int(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    environment = fields.Str(required=True)
    factors_parameters = fields.Dict(
        keys=fields.String(validate=Equal(comparable="RuntimeVariables")),
        values=fields.Nested(FactorsSchema, required=True))
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the addition of regionless records.
    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Success & None/Error - Type: JSON
    """
    current_module = "Add an all-GB region - Wrangler."
    error_message = ""

    # Define run_id outside of try block
    run_id = 0

    # Set-up variables for status message
    bpm_queue_url = None

    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        environment = runtime_variables['environment']
        factors_parameters = runtime_variables["factors_parameters"]["RuntimeVariables"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        region_column = factors_parameters["region_column"]
        regionless_code = factors_parameters["regionless_code"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey = runtime_variables['survey']

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id,
                                                           context=context)
        raise exception_classes.LambdaFailure(error_message)

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context)
        raise exception_classes.LambdaFailure(error_message)

    try:

        logger.info("Started - retrieved configuration variables.")

        # Set up clients
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        # Get data from module that preceded this step
        input_data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Successfully retrieved input data from s3")

        payload = {
            "RuntimeVariables": {
                "bpm_queue_url": bpm_queue_url,
                "environment": environment,
                "data": json.loads(
                    input_data.to_json(orient="records")),
                "regionless_code": regionless_code,
                "region_column": region_column,
                "run_id": run_id,
                "survey": survey
            }
        }

        # Pass the data for processing (adding of the regionless region)
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload),
        )
        logger.info("Successfully invoked method.")

        json_response = json.loads(imputed_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        # Save
        aws_functions.save_to_s3(bucket_name, out_file_name, json_response["data"])
        logger.info("Successfully sent data to s3.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, in_file_name))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(sns_topic_arn, "Add a all-GB region.")
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
