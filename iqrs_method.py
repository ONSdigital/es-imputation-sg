import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import Schema, fields

from imputation_functions import produce_columns


class RuntimeSchema(Schema):
    data = fields.List(fields.Dict, required=True)
    distinct_values = fields.List(fields.String, required=True)
    questions_list = fields.List(fields.String, required=True)


def lambda_handler(event, context):
    """
    Returns JSON data with new IQR columns and respective values.
    :param event: JSON payload that contains: json_data, questions_list, distinct_values.
                  Type: JSON.
    :param context: N/A.
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "IQRS - Method"
    error_message = ""
    logger = logging.getLogger("IQRS")
    run_id = 0
    try:

        logger.info("IQRS Method Begun")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables, errors = RuntimeSchema().load(event["RuntimeVariables"])
        if errors:
            logger.error(f"Error validating runtime params: {errors}")
            raise ValueError(f"Error validating runtime params: {errors}")

        logger.info("Validated parameters.")

        # Runtime Variables
        distinct_values = runtime_variables["distinct_values"]
        input_data = pd.DataFrame(runtime_variables["data"])
        questions_list = runtime_variables["questions_list"]

        logger.info("Retrieved configuration variables.")

        movement_columns = produce_columns("movement_", questions_list)
        iqrs_columns = produce_columns("iqrs_", questions_list)

        iqrs_df = calc_iqrs(
            input_data,
            movement_columns,
            iqrs_columns,
            distinct_values
        )

        logger.info("Successfully finished calculations of IQRS.")

        json_out = iqrs_df.to_json(orient="records")
        final_output = {"data": json_out}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output


def calc_iqrs(input_table, move_cols, iqrs_cols, distinct_values):
    """
    Calculate IQRS.
    :param input_table: Input DataFrame. - Type: DataFrame
    :param move_cols: Movement column list. - Type: List
    :param iqrs_cols: IQRS column list. - Type: List
    :param distinct_values: Array of column names to derive distinct values from
                            and store in table. - Type: List
    :return: Table. - Type: DataFrame
    """
    distinct_strata_region = input_table[distinct_values].drop_duplicates()

    for row in distinct_strata_region.values:
        iqr_filter = ""
        for value in distinct_values:
            if value != distinct_values[0]:
                iqr_filter += " & "
            iqr_filter += "(%s == '%s')" \
                          % (value, row[distinct_values.index(value)])

        filtered_iqr = input_table.query(str(iqr_filter))

        # Pass the question number and region and strata grouping to the iqr_sum function.
        for i in range(0, len(iqrs_cols)):

            val_one = iqr_sum(filtered_iqr, move_cols[i])

            config = {iqrs_cols[i]: val_one}

            input_table = input_table.query(str(iqr_filter))\
                .assign(**config).combine_first(input_table)

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

    if df_size % 2 == 0:
        sorted_df = df.sort_values()
        df = sorted_df.reset_index(drop=True)
        df_bottom = df[0:math.ceil(int(df_size / 2))].median()
        df_top = df[math.ceil(int(df_size / 2)):].median()
        iqr = df_top - df_bottom
    else:
        sorted_df = df.sort_values()
        df = sorted_df.reset_index(drop=True)
        q1 = df[(math.ceil(0.25 * (df_size + 1))) - 1]
        q3 = df[(math.floor(0.75 * (df_size + 1))) - 1]
        iqr = q3 - q1

    return iqr
