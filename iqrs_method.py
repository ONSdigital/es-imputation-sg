import json
import os
import traceback

import marshmallow
import numpy as np
import pandas as pd


class InputSchema(marshmallow.Schema):
    iqrs_columns = marshmallow.fields.Str(required=True)
    movement_columns = marshmallow.fields.Str(required=True)


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def lambda_handler(event, context):

    # env vars
    config, errors = InputSchema().load(os.environ)
    if errors:
        raise ValueError(f"Error validating environment params: {errors}")

    try:
        input_data = pd.read_json(event)

        iqrs_df = calc_iqrs(
            input_data,
            config['movement_columns'].split(','),
            config['iqrs_columns'].split(',')
        )

        json_out = iqrs_df.to_json(orient='records')
        final_output = json.loads(json_out)

    except Exception as exc:

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

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
    '''
    Inputs:
    df - Working dataset with the month on month question value movements
    filtered by each individual combination of region and strata.

    quest - Individual question no

    Returns:
    Returns the iqr for the question value based on the region, strata
    and question number being passed through.
    '''

    # df=dfTest
    df = df[quest]

    df_size = df.size
    import math
    if (df_size % 2 == 0):

        sorted_df = df.sort_values()
        df = sorted_df.reset_index(drop=True)
        dfbottom = df[0:math.ceil(int(df_size / 2))].median()
        dftop = df[math.ceil(int(df_size / 2)):].median()
        iqr = dftop - dfbottom
        # iqr = quantile75 - quantile25
    else:
        sorted_df = df.sort_values()
        df = sorted_df.reset_index(drop=True)
        q1 = df[(math.ceil(0.25 * (df_size + 1))) - 1]
        q3 = df[(math.floor(0.75 * (df_size + 1))) - 1]
        iqr = q3 - q1

    return iqr
