import json
import os
import traceback

import marshmallow
import numpy as np
import pandas as pd


class InputSchema(marshmallow.Schema):
    atypical_columns = marshmallow.fields.Str(required=True)
    iqrs_columns = marshmallow.fields.Str(required=True)
    movement_columns = marshmallow.fields.Str(required=True)
    mean_columns = marshmallow.fields.Str(required=True)


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

        atypicals_df = calc_atypicals(
            input_data,
            config['atypical_columns'].split(','),
            config['movement_columns'].split(','),
            config['iqrs_columns'].split(','),
            config['mean_columns'].split(',')
        )

        json_out = atypicals_df.to_json(orient='records')

        final_output = json.loads(json_out)

    except Exception as exc:

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

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
