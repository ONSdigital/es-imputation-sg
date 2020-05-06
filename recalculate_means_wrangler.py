import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import Schema, fields


class EnvironmentSchema(Schema):
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    run_environment = fields.Str(required=True)


class RuntimeSchema(Schema):
    distinct_values = fields.List(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group_id = fields.Str(required=True)
    location = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    outgoing_message_group_id = fields.Str(required=True)
    questions_list = fields.List(required=True)
    sns_topic_arn = fields.Str(required=True)
    queue_url = fields.Str(required=True)


def lambda_handler(event, context):
    """
    prepares the data for the Means method.
    - Read in data from the SQS queue.
    - Invokes the Mean Method.
    - Send data received back from the Mean method to the SQS queue.

    :param event:
    :param context:
    :return: Success & Checkpoint/Error - Type: JSON
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
        sqs = boto3.client("sqs", "eu-west-2")
        lambda_client = boto3.client("lambda", "eu-west-2")

        environment_variables, errors = EnvironmentSchema().load(os.environ)
        if errors:
            logger.error(f"Error validating environment params: {errors}")
            raise ValueError(f"Error validating environment params: {errors}")

        runtime_variables, errors = RuntimeSchema().load(event["RuntimeVariables"])
        if errors:
            logger.error(f"Error validating runtime params: {errors}")
            raise ValueError(f"Error validating runtime params: {errors}")

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        checkpoint = environment_variables["checkpoint"]
        method_name = environment_variables["method_name"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        distinct_values = runtime_variables["distinct_values"]
        in_file_name = runtime_variables["in_file_name"]
        incoming_message_group_id = runtime_variables["incoming_message_group_id"]
        location = runtime_variables["location"]
        out_file_name = runtime_variables["out_file_name"]
        outgoing_message_group_id = runtime_variables["outgoing_message_group_id"]
        questions_list = runtime_variables["questions_list"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        sqs_queue_url = runtime_variables["queue_url"]

        logger.info("Retrieved configuration variables.")

        data, receipt_handler = aws_functions.get_dataframe(
            sqs_queue_url,
            bucket_name,
            in_file_name,
            incoming_message_group_id, location
        )

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

        aws_functions.save_data(bucket_name, out_file_name,
                                json_response["data"], sqs_queue_url,
                                outgoing_message_group_id, location)
        logger.info("Successfully sent data to s3.")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)
            logger.info("Successfully deleted message from sqs.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, in_file_name, location))
            logger.info("Successfully deleted input data from s3.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                       "Imputation - Recalculate Means.")
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
