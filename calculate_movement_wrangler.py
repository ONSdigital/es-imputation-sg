import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


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
    previous_period_file = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    reference = fields.Str(required=True)
    response_type = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    time = fields.Str(required=True)


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
    error_message = ''
    log_message = ''
    checkpoint = 0

    try:
        logger.info("Starting " + current_module)

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Set up clients
        sqs = boto3.client('sqs', region_name='eu-west-2')
        lambda_client = boto3.client('lambda', region_name="eu-west-2")
        logger.info("Setting-up environment configs")

        # Event vars
        movement_type = event['RuntimeVariables']["movement_type"]

        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        in_file_name = config["in_file_name"]
        incoming_message_group = config['incoming_message_group']
        method_name = config['method_name']
        out_file_name = config["out_file_name"]
        period = event['RuntimeVariables']['period']
        perodicity = event['RuntimeVariables']['perodicity']
        period_column = event['RuntimeVariables']['period_column']
        questions_list = config['questions_list']
        previous_period_file = config['previous_period_file']
        response_type = config['response_type']  # Set as "response_type"
        sns_topic_arn = config['sns_topic_arn']
        sqs_message_group_id = config['sqs_message_group_id']
        sqs_queue_url = config['sqs_queue_url']
        time = config['time']  # Set as "period"
        reference = config['reference']  # Set as "responder_id"

        previous_period_data = aws_functions.read_dataframe_from_s3(
            bucket_name, previous_period_file)
        logger.info("Completed reading data from s3")

        data, receipt_handler = aws_functions.get_dataframe(sqs_queue_url, bucket_name,
                                                            in_file_name,
                                                            incoming_message_group)
        logger.info("Successfully retrieved data")
        # Create a Dataframe where the response column
        # value is set as 1 i.e non responders
        filtered_non_responders = data.loc[(data[response_type] == 1) &
                                           (data[time] == int(period))]

        logger.info("Successfully created filtered non responders DataFrame")

        response_check = len(filtered_non_responders.index)

        # If greater than 0 it means there is non-responders so Imputation need to be run
        if response_check > 0:

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

            for question in questions_list.split(','):
                merged_data['movement_' + question] = 0.0

            json_ordered_data = merged_data.to_json(orient='records')

            json_payload = {
                "json_data": json_ordered_data,
                "movement_type": movement_type,
                "questions_list": questions_list,
                "current_period": period,
                "period_column": period_column,
                "perodicity": perodicity
            }

            logger.info("Successfully created movement columns on the data")

            imputed_data = lambda_client.invoke(FunctionName=method_name,
                                                Payload=json.dumps(json_payload))

            logger.info("Succesfully invoked method.")

            json_response = json.loads(imputed_data.get('Payload').read().decode("UTF-8"))
            logger.info("JSON extracted from method response.")

            if not json_response['success']:
                raise exception_classes.MethodFailure(json_response['error'])

            imputation_run_type = "Calculate Movement."
            aws_functions.save_data(bucket_name, out_file_name,
                                    json_response["data"], sqs_queue_url,
                                    sqs_message_group_id)

            logger.info("Successfully sent the data to s3")

        else:

            to_be_imputed = False
            imputation_run_type = "Has Not Run."

            aws_functions.save_data(
                bucket_name,
                in_file_name,
                data.to_json(orient="records"),
                sqs_queue_url,
                sqs_message_group_id
            )

            logger.info("Successfully sent the unchanged data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        aws_functions.send_sns_message(
            checkpoint, sns_topic_arn,
            'Imputation - ' + imputation_run_type)

        logger.info("Successfully sent the SNS message")

    except AttributeError as e:
        error_message = "Bad data encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = "Parameter validation error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = "AWS Error (" \
                        + str(e.response['Error']['Code']) + ") " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = "Incomplete Lambda response encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except exception_classes.MethodFailure as e:
        error_message = e.error_message
        log_message = "Error in " + method_name + "."
    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    return {
        "success": True,
        "checkpoint": checkpoint,
        "impute": to_be_imputed
    }
