import json
import logging
import os

import boto3
import pandas as pd
from esawsfunctions import funk
from botocore.exceptions import ClientError, IncompleteReadError
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    arn = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    checkpoint = fields.Str(required=True)
    method_name = fields.Str(required=True)
    non_responder_file = fields.Str(required=True)
    period = fields.Str(required=True)
    queue_url = fields.Str(required=True)
    s3_file = fields.Str(required=True)
    sqs_messageid_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    file_name = fields.Str(required=True)

class NoDataInQueueError(Exception):
    pass


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the apply factors statistical method.
    The method requires a column per question to store the factors.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    current_module = "Apply Factors - Wrangler"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Apply")
    logger.setLevel(10)
    try:
        logger.info("Apply Factors Wrangler Begun")
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        # Set up clients
        # # S3
        bucket_name = config["bucket_name"]
        non_responder_data_file = config["non_responder_file"]
        # Sqs
        queue_url = config["queue_url"]
        sqs_messageid_name = config["sqs_messageid_name"]

        #
        incoming_message_group = config["incoming_message_group"]
        file_name = config["file_name"]
        checkpoint = config["checkpoint"]
        current_period = config["period"]
        method_name = config["method_name"]
        #

        s3_file = config["s3_file"]
        arn = config["arn"]
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        factors_dataframe, receipt_handler = funk.get_dataframe(queue_url, bucket_name, "calc_out.json", incoming_message_group)
        logger.info("Successfully retrieved data from sqs")

        # Reads in non responder data
        non_responder_dataframe = funk.read_dataframe_from_s3(bucket_name, "non_responders_output.json")

        logger.info("Successfully retrieved non-responder data from s3")

        # Read in previous period data for current period non-responders
        prev_period_data = funk.read_dataframe_from_s3(bucket_name, s3_file)
        logger.info("Successfully retrieved previous period data from s3")
        # Filter so we only have those that responded in prev
        prev_period_data = prev_period_data[prev_period_data["response_type"] == 2]

        question_columns = [
            "Q601_asphalting_sand",
            "Q602_building_soft_sand",
            "Q603_concreting_sand",
            "Q604_bituminous_gravel",
            "Q605_concreting_gravel",
            "Q606_other_gravel",
            "Q607_constructional_fill",
        ]
        prev_question_columns = [
            "prev_Q601_asphalting_sand",
            "prev_Q602_building_soft_sand",
            "prev_Q603_concreting_sand",
            "prev_Q604_bituminous_gravel",
            "prev_Q605_concreting_gravel",
            "prev_Q606_other_gravel",
            "prev_Q607_constructional_fill",
            "responder_id",
        ]

        for question in question_columns:
            prev_period_data = prev_period_data.rename(
                index=str, columns={question: "prev_" + question}
            )
        logger.info("Successfully renamed previous period data")
        # Join prev data so we have those who responded in prev but not in current

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
                [
                    "region",
                    "strata",
                    "imputation_factor_Q601_asphalting_sand",
                    "imputation_factor_Q602_building_soft_sand",
                    "imputation_factor_Q603_concreting_sand",
                    "imputation_factor_Q604_bituminous_gravel",
                    "imputation_factor_Q605_concreting_gravel",
                    "imputation_factor_Q606_other_gravel",
                    "imputation_factor_Q607_constructional_fill",
                ]
            ],
            on=["region", "strata"],
            how="inner",
        )
        logger.info("Successfully merged non-responders with factors")
        # Non responder data should now contain all previous values and 7 imp columns
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=non_responders_with_factors.to_json(orient="records"),
        )

        json_response = json.loads(imputed_data.get("Payload").read().decode("ascii"))
        logger.info("Successfully invoked lambda")

        # This bit will want a fix
        imputed_non_responders = pd.read_json(str(json_response).replace("'", '"'))
        current_responders = factors_dataframe[
            factors_dataframe["period"] == int(current_period)
        ]

        final_imputed = pd.concat([current_responders, imputed_non_responders])
        logger.info("Successfully joined imputed data with responder data")
        imputation_run_type = "Imputation complete"
        message = final_imputed.to_json(orient="records")

        funk.save_data(bucket_name, file_name, message, queue_url, sqs_messageid_name)
        logger.info("Successfully sent to sqs")
        funk.send_sns_message(checkpoint, imputation_run_type, arn)
        logger.info("Successfully sent to sns")
        logger.info(funk.delete_data(bucket_name, file_name))

    except TypeError as e:
        error_message = (
            "Bad data type encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Parameter validation error"
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
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
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except IncompleteReadError as e:
        error_message = (
            "Incomplete Lambda response encountered in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context["aws_request_id"])
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
            + str(context["aws_request_id"])
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {"success": True, "checkpoint": checkpoint}
