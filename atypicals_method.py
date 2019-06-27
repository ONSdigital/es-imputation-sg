import traceback
import json
import boto3
import os
import pandas as pd
import numpy as np

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

    # Set up clients
    lambda_client = boto3.client('lambda')

    error_handler_arn = os.environ['error_handler_arn']
    atypical_columns = os.environ['atypical_columns']
    iqrs_columns = os.environ['iqrs_columns']
    movement_columns = os.environ['movement_columns']
    mean_columns = os.environ['mean_columns']


    try:
        input_data = pd.read_json(event)

        atypicals_df = calc_atypicals(input_data, atypical_columns.split(','), movement_columns.split(','), iqrs_columns.split(','),mean_columns.split(','))

        json_out = atypicals_df.to_json(orient='records')

        final_output = json.loads(json_out)

    except Exception as exc:

        # Invoke error handler lambda
        lambda_client.invoke(
            FunctionName=error_handler_arn,
            InvocationType='Event',
            Payload=json.dumps({'test': 'ccow'})
        )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return final_output


def calc_atypicals(input_table, atyp_col, move_col, iqrs_col, mean_col):

    for i in range(0, len(iqrs_col)):
        input_table[atyp_col[i]] = abs(input_table[move_col[i]] - input_table[mean_col[i]]) - 2 * input_table[iqrs_col[i]]
        input_table[atyp_col[i]] = input_table[atyp_col[i]].round(8)


    for j in range(0, len(iqrs_col)):
        input_table[move_col[j]] = np.where((input_table[atyp_col[j]] > 0), None,
                                              input_table[move_col[j]])

    return input_table


