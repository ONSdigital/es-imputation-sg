import json
import os
import marshmallow
import numpy as np
import pandas as pd
import logging


class InputSchema(marshmallow.Schema):
    atypical_columns = marshmallow.fields.Str(required=True)
    iqrs_columns = marshmallow.fields.Str(required=True)
    movement_columns = marshmallow.fields.Str(required=True)
    mean_columns = marshmallow.fields.Str(required=True)


def lambda_handler(event, context):
    """
    Add docs here.
    """
    current_module = "Atypicals - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Atypicals")
    try:

        logger.info("Atypicals Method Begun")

        # env vars
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        logger.info("Validated params.")

        input_data = pd.read_json(event)

        logger.info("Succesfully retrieved data from event.")

        atypicals_df = calc_atypicals(
            input_data,
            config['atypical_columns'].split(','),
            config['movement_columns'].split(','),
            config['iqrs_columns'].split(','),
            config['mean_columns'].split(',')
        )

        json_out = atypicals_df.to_json(orient='records')

        final_output = json.loads(json_out)

        logger.info("Succesfully calculated atypicals.")

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


def calc_atypicals(input_table, atyp_col, move_col, iqrs_col, mean_col):

    for i in range(0, len(iqrs_col)):
        input_table[atyp_col[i]] = abs(input_table[move_col[i]] - input_table[mean_col[i]]) - 2 * input_table[iqrs_col[i]]  # noqa: E501
        input_table[atyp_col[i]] = input_table[atyp_col[i]].round(8)

    for j in range(0, len(iqrs_col)):
        input_table[move_col[j]] = np.where(
            (input_table[atyp_col[j]] > 0),
            None,
            input_table[move_col[j]]
        )

    return input_table
