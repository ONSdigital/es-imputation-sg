import json
import logging
import os

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import Schema, fields


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
    This wrangler is used to prepare data for the addition of regionless records.
    :param event: Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Success & Checkpoint/Error - Type: JSON
    """
    current_module = "Add an all-GB region - Wrangler."
    error_message = ""
    logger = logging.getLogger("AllGB")
    logger.setLevel(10)
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

        # Environment Variables
        bucket_name = config["bucket_name"]
        checkpoint = config["checkpoint"]
        method_name = config["method_name"]
        run_environment = config['run_environment']

        # Runtime Variables
        factors_parameters = event['RuntimeVariables']["factors_parameters"]
        in_file_name = event['RuntimeVariables']['in_file_name']
        incoming_message_group_id = event['RuntimeVariables']['incoming_message_group_id']
        location = event['RuntimeVariables']['location']
        out_file_name = event['RuntimeVariables']['out_file_name']
        outgoing_message_group_id = event['RuntimeVariables']["outgoing_message_group_id"]
        region_column = factors_parameters['RuntimeVariables']['region_column']
        regionless_code = factors_parameters['RuntimeVariables']['regionless_code']
        sns_topic_arn = event['RuntimeVariables']["sns_topic_arn"]
        sqs_queue_url = event['RuntimeVariables']["queue_url"]

        logger.info("Retrieved configuration variables.")

        # Set up clients
        sqs = boto3.client('sqs', 'eu-west-2')
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        # Get data from module that preceded this step
        input_data, receipt_handler = aws_functions.get_dataframe(
                                                        sqs_queue_url,
                                                        bucket_name,
                                                        in_file_name,
                                                        incoming_message_group_id,
                                                        location)

        logger.info("Successfully retrieved input data from s3")

        payload = {
            "RuntimeVariables": {
                "json_data": json.loads(
                    input_data.to_json(orient="records")),
                "regionless_code": regionless_code,
                "region_column": region_column,
                "run_id": run_id
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

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        # Save
        aws_functions.save_data(bucket_name, out_file_name,
                                json_response["data"], sqs_queue_url,
                                outgoing_message_group_id, location)
        logger.info("Successfully sent data to s3.")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)
            logger.info("Successfully deleted message from sqs.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, in_file_name, location))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(checkpoint, sns_topic_arn, 'Add a all-GB region.')
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
