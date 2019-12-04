import logging

import pandas as pd

from imputation_functions import produce_columns


def lambda_handler(event, context):
    """
    Returns JSON data with new IQR columns and respective values.
    :param event: JSON payload that contains: json_data, questions_list, distinct_values.
                  Type: JSON.
    :param context: N/A.
    :return: The means data now with the respective iter quartile ranges added -Type: JSON
    """
    current_module = "IQRS - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("IQRS")
    try:

        logger.info("IQRS Method Begun")

        # Environment variables
        questions_list = event['questions_list']
        input_data = pd.DataFrame(event["data"])

        logger.info("Successfully retrieved data from event.")

        iqrs_df = calc_iqrs(
            input_data,
            produce_columns("movement_", questions_list.split(',')),
            produce_columns("iqrs_", questions_list.split(',')),
            event["distinct_values"].strip().split(',')
        )
        iqrs_df['region'] = iqrs_df['region'].astype('int64')

        logger.info("Succesfully finished calculations of IQRS.")

        json_out = iqrs_df.to_json(orient='records')
        final_output = {"data": json_out}

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
