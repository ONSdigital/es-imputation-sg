import json
import logging
import os

import boto3
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
    run_environment = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    distinct_values = fields.List(fields.String, required=True)
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    sns_topic_arn = fields.Str(required=True)


def lambda_handler(event, context):
    """
    prepares the data for the Means method.
    - Read in data from the SQS queue.
    - Invokes the Mean Method.
    - Send data received back from the Mean method to the SQS queue.

    :param event:
    :param context:
    :return: Success & None/Error - Type: JSON
    """
    current_module = "Imputation Recalculate Means - Wrangler."
    error_message = ""
    logger = logging.getLogger("RecalcMeans")
    logger.setLevel(10)
    # Define run_id outside of try block
    run_id = 0
    try:
        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Set up clients
        lambda_client = boto3.client("lambda", "eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        distinct_values = runtime_variables["distinct_values"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        questions_list = runtime_variables["questions_list"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]

        logger.info("Retrieved configuration variables.")

        data = aws_functions.read_dataframe_from_s3(bucket_name, in_file_name)

        logger.info("Successfully retrieved data")

        # Add means columns
        for question in questions_list:
            data.drop(["movement_" + question + "_count"], axis=1, inplace=True)
            data.drop(["movement_" + question + "_sum"], axis=1, inplace=True)
            data.drop(["atyp_" + question, "iqrs_" + question], axis=1, inplace=True)
            data["mean_" + question] = 0.0

        data_json = data.to_json(orient="records")

        payload = {
            "RuntimeVariables": {
                "data": json.loads(data_json),
                "distinct_values": distinct_values,
                "questions_list": questions_list,
                "run_id": run_id
            }
        }

        returned_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload)
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
            logger.info("Successfully deleted input data from s3.")

        aws_functions.send_sns_message(sns_topic_arn, "Imputation - Recalculate Means.")
        logger.info("Successfully sent message to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {"success": True}
