import logging

import numpy as np
import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields

import imputation_functions as imp_func


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url =fields.Str(required=True)
    data = fields.List(fields.Dict, required=True)
    questions_list = fields.List(fields.String, required=True)


def lambda_handler(event, context):
    """
    Returns JSON data with new Atypicals columns and respective values.
    :param event: JSON payload that contains: json_data and questions_list - Type: JSON.
    :param context: Context object.
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Imputation Atypicals - Method."
    error_message = ""
    logger = general_functions.get_logger()

    bpm_queue_url = None

    run_id = 0
    try:

        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        input_data = pd.DataFrame(runtime_variables["data"])
        questions_list = runtime_variables["questions_list"]

        logger.info("Retrieved configuration variables.")

        # Produce columns
        atypical_columns = imp_func.produce_columns("atyp_", questions_list)
        movement_columns = imp_func.produce_columns("movement_", questions_list)
        iqrs_columns = imp_func.produce_columns("iqrs_", questions_list)
        mean_columns = imp_func.produce_columns("mean_", questions_list)

        atypicals_df = calc_atypicals(
            input_data,
            atypical_columns,
            movement_columns,
            iqrs_columns,
            mean_columns
        )
        logger.info("Successfully finished calculations of atypicals.")

        json_out = atypicals_df.to_json(orient="records")

        final_output = {"data": json_out}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output


def calc_atypicals(input_table, atyp_col, move_col, iqrs_col, mean_col):
    """
    Calculates the atypical values for each column like so:
        atypical_value = (movement_value - mean_value) - 2 * iqrs_value
    This value is then rounded to 8 decimal places.
    :param input_table: DataFrame containing means/movement data - Type: DataFrame
    :param atyp_col: String containing atypical column names - Type: String
    :param move_col: String containing movement column names - Type: String
    :param irqs_col: String containing iqrs column names - Type: String
    :param mean_col: String containing means column names - Type: String
    :return input_table: with the atypicals that have been calculated appended.
    """
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
