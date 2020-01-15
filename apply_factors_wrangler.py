import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields

from imputation_functions import produce_columns


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
    period = fields.Str(required=True)
    previous_data_file = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    response_type = fields.Str(required=True)
    reference = fields.Str(required=True)
    strata_column = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the apply factors statistical method.
    The method requires a column per question to store the factors.
    :param event:  Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Success & Checkpoint/Error - Type: JSON
    """
    current_module = "Imputation Apply Factors - Wrangler."
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Apply")
    logger.setLevel(10)
    try:
        logger.info("Starting " + current_module)
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        # Event vars
        distinct_values = event['RuntimeVariables']["distinct_values"]
        sum_columns = event['RuntimeVariables']["sum_columns"]
        period_column = event['RuntimeVariables']['period_column']
        factors_parameters = event["RuntimeVariables"]["factors_parameters"]
        regionless_code = factors_parameters["RuntimeVariables"]['regionless_code']
        region_column = factors_parameters["RuntimeVariables"]['region_column']
        raw_input_file \
            = event['RuntimeVariables']['raw_input_file']

        # Environment vars
        checkpoint = config["checkpoint"]
        bucket_name = config["bucket_name"]
        current_period = config["period"]
        incoming_message_group = config["incoming_message_group"]
        in_file_name = config["in_file_name"]
        method_name = config["method_name"]
        out_file_name = config["out_file_name"]
        previous_data_file = config["previous_data_file"]
        questions_list = config["questions_list"]
        sns_topic_arn = config["sns_topic_arn"]
        sqs_queue_url = config["sqs_queue_url"]
        response_type = config['response_type']
        reference = config['reference']
        strata_column = config['strata_column']
        sqs = boto3.client('sqs', 'eu-west-2')
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        # Get data from module that preceded imputation
        input_data = aws_functions.read_dataframe_from_s3(bucket_name, raw_input_file)
        # Split out non responder data from input
        non_responder_dataframe = input_data[input_data[response_type] == 1]
        logger.info("Successfully retrieved raw-input data from s3")

        # Get factors data from calculate_factors
        factors_dataframe, receipt_handler = aws_functions.get_dataframe(
            sqs_queue_url, bucket_name, in_file_name, incoming_message_group)
        logger.info("Successfully retrieved factors data from s3")

        # Read in previous period data for current period non-responders
        prev_period_data = aws_functions.read_dataframe_from_s3(bucket_name,
                                                                previous_data_file)
        logger.info("Successfully retrieved previous period data from s3")
        # Filter so we only have those that responded in prev
        prev_period_data = prev_period_data[prev_period_data[response_type] == 2]

        questions_list = questions_list.split(",")
        prev_questions_list = produce_columns(
            "prev_",
            questions_list,
            [reference]
        )

        for question in questions_list:
            prev_period_data = prev_period_data.rename(
                index=str, columns={question: "prev_" + question}
            )
        logger.info("Successfully renamed previous period data")

        non_responder_dataframe_with_prev = pd.merge(
            non_responder_dataframe,
            prev_period_data[prev_questions_list],
            on=reference,
        )
        logger.info("Successfully merged previous period data with non-responder df")

        # filter factors df to only get current period
        factors_dataframe = factors_dataframe[
            factors_dataframe[period_column] == int(current_period)]

        # Merge the factors onto the non responders
        non_responders_with_factors = pd.merge(
            non_responder_dataframe_with_prev,
            factors_dataframe[
                produce_columns(
                    "imputation_factor_",
                    questions_list,
                    distinct_values
                )
            ],
            on=distinct_values,
            how="inner",
        )
        logger.info("Successfully merged non-responders with factors")

        # Region/strata combinations that exist in responder data
        # but not non_responders get dropped off on join
        # With factors. Identify these, merge on the regionless factor.
        # Then concat onto original dataset.
        # It looked a lot nicer before flake8....
        dropped_rows = non_responder_dataframe_with_prev[
            ~non_responder_dataframe_with_prev[reference].isin(
                non_responders_with_factors[reference])].dropna()
        if(len(dropped_rows) > 0):
            if(strata_column in dropped_rows.columns.values):
                regionless_factors = \
                    factors_dataframe[
                        produce_columns("imputation_factor_",
                                        questions_list,
                                        distinct_values)
                    ][factors_dataframe[region_column] == regionless_code]
                regionless_factors = regionless_factors.drop(
                    [region_column], inplace=False, axis=1)
                dropped_rows_with_factors = \
                    pd.merge(dropped_rows, regionless_factors, on='strata', how="inner")
                non_responders_with_factors = \
                    pd.concat([non_responders_with_factors, dropped_rows_with_factors])
                logger.info("Successfully merged missing rows with non_responders")

        payload = {
            "json_data": json.loads(
                non_responders_with_factors.to_json(orient="records")),
            "questions_list": questions_list,
            "sum_columns": sum_columns
        }

        # Non responder data should now contain all previous values
        #   and the imputation columns
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload),
        )
        logger.info("Succesfully invoked method.")

        json_response = json.loads(imputed_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response['success']:
            raise exception_classes.MethodFailure(json_response['error'])

        imputed_non_responders = pd.read_json(json_response["data"], dtype=False)

        # retrieve current responders from input data..
        current_responders = input_data[
            input_data[response_type] == 2
            ]

        # Joining Datasets Together.
        final_imputed = pd.concat([current_responders, imputed_non_responders])
        logger.info("Successfully joined imputed data with responder data")

        # rows in current period not suitable for imputation
        # (eg, no matching row in previous, or non_responder in previous)
        # Need to be joined back onto the final output
        dropped_rows = non_responder_dataframe[
            ~non_responder_dataframe[reference].
            isin(final_imputed[reference])].dropna()
        final_imputed = pd.concat([final_imputed, dropped_rows])
        logger.info("Successfully joined tacked on unimputable data")

        # Create A List Of Factor Columns To Drop
        cols_to_drop = produce_columns("imputation_factor_", questions_list,
                                       produce_columns("prev_", questions_list))

        filtered_data = final_imputed.drop(cols_to_drop, axis=1)

        message = filtered_data.to_json(orient="records")

        aws_functions.save_to_s3(bucket_name, out_file_name, message)
        logger.info("Successfully sent data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        aws_functions.send_sns_message(checkpoint, sns_topic_arn,
                                       'Imputation - Apply Factors.')
        logger.info("Successfully sent message to sns")
        logger.info(aws_functions.delete_data(bucket_name, in_file_name))

    except TypeError as e:
        error_message = (
            "Bad data type encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
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
