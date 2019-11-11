import json
import logging
import os

import numpy as np
import pandas as pd
from marshmallow import Schema, fields


class InputSchema(Schema):
    iqrs_columns = fields.Str(required=True)
    movement_columns = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Returns JSON data with new IQR columns and respective values.
    :param event: Event object
    :param contet: Contet object

    :return: JSON string
    """
    current_module = "IQRS - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("IQRS")
    try:

        logger.info("IQRS Method Begun")

        # env vars
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params.")

        input_data = pd.read_json(event)

        logger.info("Succesfully retrieved data from event.")

        iqrs_df = calc_iqrs(
            input_data,
            config['movement_columns'].split(','),
            config['iqrs_columns'].split(',')
        )

        json_out = iqrs_df.to_json(orient='records')
        final_output = json.loads(json_out)

    except ValueError as e:
        error_message = (
            "Input Error in "
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
            return final_output


def calc_iqrs(input_table, move_cols, iqrs_cols):
    distinct_strata_region = input_table[['region', 'strata']].drop_duplicates()
    for row in distinct_strata_region.values:
        iqr_filter = (input_table["region"] == row[0]) & (input_table["strata"] == row[1])  # noqa: E501
        filtered_iqr = input_table[iqr_filter]
        # Pass the question number and region and strata groupping to the
        # iqr_sum function.
        for i in range(0, len(iqrs_cols)):
            val_one = iqr_sum(filtered_iqr, move_cols[i])
            input_table[iqrs_cols[i]] = np.where(
                ((input_table["region"] == row[0]) & (input_table["strata"] == row[1])), val_one,  # noqa: E501
                input_table[iqrs_cols[i]]
            )
    return input_table


def iqr_sum(df, quest):
    """
    :param df: Working dataset with the month on month question value movements
    filtered by each individual combination of region and strata - Type: DataFrame
    :param quest: Individual question no - Type: String

    :return: String
    """

    df = df[quest]

    df_size = df.size
    import math
    if (df_size % 2 == 0):

        sorted_df = df.sort_values()
        df = sorted_df.reset_index(drop=True)
        dfbottom = df[0:math.ceil(int(df_size / 2))].median()
        dftop = df[math.ceil(int(df_size / 2)):].median()
        iqr = dftop - dfbottom
    else:
        sorted_df = df.sort_values()
        df = sorted_df.reset_index(drop=True)
        q1 = df[(math.ceil(0.25 * (df_size + 1))) - 1]
        q3 = df[(math.floor(0.75 * (df_size + 1))) - 1]
        iqr = q3 - q1

    return iqr
