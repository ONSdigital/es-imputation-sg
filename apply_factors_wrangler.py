import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    non_responder_file = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    period = fields.Str(required=True)
    previous_data_file = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    question_columns = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the apply factors statistical method.
    The method requires a column per question to store the factors.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
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
        distinct_values = event["distinct_values"]

        # Set up clients
        checkpoint = config["checkpoint"]
        bucket_name = config["bucket_name"]
        current_period = config["period"]
        incoming_message_group = config["incoming_message_group"]
        in_file_name = config["in_file_name"]
        method_name = config["method_name"]
        non_responder_data_file = config["non_responder_file"]
        out_file_name = config["out_file_name"]
        previous_data_file = config["previous_data_file"]
        sns_topic_arn = config["sns_topic_arn"]
        sqs_queue_url = config["sqs_queue_url"]
        question_columns = config["question_columns"]

        sqs = boto3.client('sqs', 'eu-west-2')
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        factors_dataframe, receipt_handler = funk.get_dataframe(
            sqs_queue_url, bucket_name, in_file_name, incoming_message_group)
        logger.info("Successfully retrieved data from sqs")

        # Reads in non responder data
        non_responder_dataframe = funk.read_dataframe_from_s3(
            bucket_name, non_responder_data_file)

        logger.info("Successfully retrieved non-responder data from s3")

        # Read in previous period data for current period non-responders
        prev_period_data = funk.read_dataframe_from_s3(bucket_name, previous_data_file)
        logger.info("Successfully retrieved previous period data from s3")
        # Filter so we only have those that responded in prev
        prev_period_data = prev_period_data[prev_period_data["response_type"] == 2]

        question_columns = question_columns.split(",")
        prev_question_columns = produce_columns(
            "prev_",
            question_columns,
            ['responder_id']
        )

        for question in question_columns:
            prev_period_data = prev_period_data.rename(
                index=str, columns={question: "prev_" + question}
            )
        logger.info("Successfully renamed previous period data")

        non_responder_dataframe = pd.merge(
            non_responder_dataframe,
            prev_period_data[prev_question_columns],
            on="responder_id",
        )
        logger.info("Successfully merged previous period data with non-responder df")
        # Merge the factors onto the non responders
        non_responders_with_factors = pd.merge(
            non_responder_dataframe,
            factors_dataframe[
                    produce_columns(
                        "imputation_factor_",
                        question_columns,
                        distinct_values
                    )
            ],
            on=distinct_values,
            how="inner",
        )
        logger.info("Successfully merged non-responders with factors")

        payload = {
            "json_data": non_responders_with_factors.to_json(orient="records"),
            "question_columns": question_columns
        }

        # Non responder data should now contain all previous values and 7 imp columns
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.loads(payload),
        )

        json_response = json.loads(imputed_data.get("Payload").read().decode("ascii"))
        logger.info("Successfully invoked lambda")

        # ----- This bit will want a fix. ----- #
        imputed_non_responders = pd.read_json(str(json_response).replace("'", '"'))
        # ------------------------------------- #

        # Filtering Data To Be Current Period Only.
        current_responders = factors_dataframe[
            factors_dataframe["period"] == int(current_period)
        ]

        # Joining Datasets Together.
        final_imputed = pd.concat([current_responders, imputed_non_responders])
        logger.info("Successfully joined imputed data with responder data")

        # Filter Out The Data Columns From The Temporary Calculated Columns.
        filtered_data = final_imputed[['Q601_asphalting_sand', 'Q602_building_soft_sand',
                                       'Q603_concreting_sand', 'Q604_bituminous_gravel',
                                       'Q605_concreting_gravel', 'Q606_other_gravel',
                                       'Q607_constructional_fill', 'Q608_total', 'county',
                                       'county_name', 'enterprise_ref', 'gor_code',
                                       'land_or_marine', 'name', 'period', 'region',
                                       'responder_id', 'strata']]

        message = filtered_data.to_json(orient="records")

        funk.save_to_s3(bucket_name, out_file_name, message)
        logger.info("Successfully sent data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        funk.send_sns_message(checkpoint, sns_topic_arn, 'Imputation - Apply Factors.')
        logger.info("Successfully sent message to sns")
        logger.info(funk.delete_data(bucket_name, in_file_name))

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
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}


def produce_columns(prefix, columns, suffix):
    """
    Produces columns with a prefix, based on standard columns.
    :param prefix: String to be prepended to column name - Type: String
    :param columns: List of columns - Type: List
    :param suffix: Any additonal columns to be added on - Type: List

    :return: List of column names with desired prefix - Type: List
    """
    new_columns = []
    for column in columns:
        new_value = "%s%s" % (prefix, column)
        new_columns.append(new_value)

    new_columns = new_columns + suffix

    return new_columns
