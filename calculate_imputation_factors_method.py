import json
import logging
import os

import pandas as pd
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    first_threshold = fields.Str(required=True)
    second_threshold = fields.Str(required=True)
    third_threshold = fields.Str(required=True)
    first_imputation_factor = fields.Str(required=True)
    second_imputation_factor = fields.Str(required=True)
    third_imputation_factor = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Calculates imputation factor for each question, in each aggregated group.
    :param event: JSON payload that contains: json_data, questions_list - Type: JSON.
    :param context: lambda context
    :return: final_output - Type: JSON
    """
    current_module = "Calculate Factors - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("CalculateFactors")
    logger.setLevel(10)
    try:
        logger.info("Calculate Factors Method Begun")
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params")

        # set up variables
        questions_list = event["questions_list"]
        first_threshold = config["first_threshold"]
        second_threshold = config["second_threshold"]
        third_threshold = config["third_threshold"]
        first_imputation_factor = config["first_imputation_factor"]
        second_imputation_factor = config["second_imputation_factor"]
        third_imputation_factor = config["third_imputation_factor"]

        df = pd.DataFrame(event["data_json"])

        def calculate_imputation_factors(row, question):
            """
            Calculates the imputation factors for the DataFrame on row by row basis.
            - Calculates imputation factor for each question, in each aggregated group,
              by:
                Region
                Land or Marine (If applicable)
                Count of refs within cell

            :param row: row of DataFrame
            :param question: question
            :return: row of DataFrame
            """
            if row["region"] == 14:
                if row["land_or_marine"] == "L":
                    if row["movement_" + question + "_count"] < int(first_threshold):
                        row["imputation_factor_" + question] = int(
                            first_imputation_factor
                        )
                    else:
                        row["imputation_factor_" + question] = row["mean_" + question]
                else:
                    if row["movement_" + question + "_count"] < int(second_threshold):
                        row["imputation_factor_" + question] = int(
                            second_imputation_factor
                        )
                    else:
                        row["imputation_factor_" + question] = row["mean_" + question]
            else:
                if row["movement_" + question + "_count"] < int(third_threshold):
                    row["imputation_factor_" + question] = int(third_imputation_factor)
                else:
                    row["imputation_factor_" + question] = row["mean_" + question]

            return row

        for question in questions_list.split(","):
            df = df.apply(lambda x: calculate_imputation_factors(x, question), axis=1)
            logger.info("Calculated Factors for " + str(question))
        factors_dataframe = df

        logger.info("Succesfully calculated factors")

        final_output = {"data": json.loads(factors_dataframe.to_json(orient="records"))}

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
    final_output["success"] = True
    return final_output
