import os
import traceback

import boto3  # noqa F401
import pandas as pd
from marshmallow import Schema, fields


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.

    :param exception: Exception object
    :return: string
    """
    return "".join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


class EnvironSchema(Schema):
    questions = fields.Str(required=True)
    first_threshold = fields.Str(required=True)
    second_threshold = fields.Str(required=True)
    third_threshold = fields.Str(required=True)
    first_imputation_factor = fields.Str(required=True)
    second_imputation_factor = fields.Str(required=True)
    third_imputation_factor = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Calculates the imputation factors.
    Called by the Calculate imputation factors wrangler.

    :param event: lambda event
    :param context: lambda context
    :return: json dataset
    """

    try:
        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # set up variables
        questions = config["questions"]
        first_threshold = config["first_threshold"]
        second_threshold = config["second_threshold"]
        third_threshold = config["third_threshold"]
        first_imputation_factor = config["first_imputation_factor"]
        second_imputation_factor = config["second_imputation_factor"]
        third_imputation_factor = config["third_imputation_factor"]

        df = pd.DataFrame(event)

        def calculate_imputation_factors(row, question):
            """
            Calculates the imputation factors for the DataFrame.
            Does this on row by row basis.
            - Calculates imputation factor per question in each aggregated group, by:
                Region
                Land or Marine
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

        for question in questions.split(" "):
            df = df.apply(lambda x: calculate_imputation_factors(x, question), axis=1)
        factors_dataframe = df

    except Exception as exc:

        return {
            "success": False,
            "error": "Unexpected method exception {}".format(_get_traceback(exc)),
        }

    final_output = factors_dataframe.to_json(orient="records")

    return final_output
