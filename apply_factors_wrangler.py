import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
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
    non_responder_file = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    period = fields.Str(required=True)
    previous_data_file = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    questions_list = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the apply factors statistical method.
    The method requires a column per question to store the factors.
    :param event:  Contains all the variables which are required for the specific run.
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
        distinct_values = event['RuntimeVariables']["distinct_values"].split(",")

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
        questions_list = config["questions_list"]

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

        questions_list = questions_list.split(",")
        prev_questions_list = produce_columns(
            "prev_",
            questions_list,
            ['responder_id']
        )
        pd.set_option('display.max_columns', 30)
        for question in questions_list:
            prev_period_data = prev_period_data.rename(
                index=str, columns={question: "prev_" + question}
            )
        logger.info("Successfully renamed previous period data")

        non_responder_dataframe = pd.merge(
            non_responder_dataframe,
            prev_period_data[prev_questions_list],
            on="responder_id",
        )
        logger.info("Successfully merged previous period data with non-responder df")
        # Merge the factors onto the non responders
        non_responders_with_factors = pd.merge(
            non_responder_dataframe,
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

        payload = {
            "json_data": json.loads(
                non_responders_with_factors.to_json(orient="records")),
            "questions_list": questions_list
        }

        # Non responder data should now contain all previous values and 7 imp columns
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload),
        )

        json_response = json.loads(imputed_data.get("Payload").read().decode("UTF-8"))
        if str(type(json_response)) != "<class 'str'>":
            raise funk.MethodFailure(json_response['error'])
        logger.info("Successfully invoked lambda")

        imputed_non_responders = pd.DataFrame(json.loads(json_response))

        # Filtering Data To Be Current Period Only.
        current_responders = factors_dataframe[
            factors_dataframe["period"] == int(current_period)
        ]

        # Joining Datasets Together.
        final_imputed = pd.concat([current_responders, imputed_non_responders])

        logger.info("Successfully joined imputed data with responder data")

        # Create A List Of Movement Columns Plus (
        #   A List Of Means Columns Plus (
        #       A List Of Factor Columns Plus (
        #           A List Of Movement Sum Columns Plus (
        #               A List Of Movement Count Columns))))
        # See Mike For Why This Way And Not Variables Appended Together.
        cols_to_drop = produce_columns(
            "movement_",
            questions_list,
            produce_columns(
                "mean_",
                questions_list,
                produce_columns(
                    "imputation_factor_",
                    questions_list,
                    produce_columns(
                        "movement_",
                        questions_list,
                        produce_columns(
                            "movement_",
                            questions_list,
                            suffix="_count"
                        ), suffix="_sum"
                    )
                )
            )
        )

        filtered_data = final_imputed.drop(cols_to_drop, axis=1)

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
    except funk.MethodFailure as e:
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
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
