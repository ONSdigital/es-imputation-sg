import traceback
import json
import boto3
import os
import pandas as pd
import numpy as np

# Set up clients

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
    lambda_client = boto3.client('lambda')

    error_handler_arn = os.environ['error_handler_arn']

    iqrs_columns = os.environ['iqrs_columns']
    movement_columns = os.environ['movement_columns']

    try:
        input_data = pd.read_json(event)

        iqrs_df = calc_iqrs(input_data, movement_columns.split(','), iqrs_columns.split(','))

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
        iqr_filter = (input_table["region"] == row[0]) & (input_table["strata"] == row[1])
        filtered_iqr = input_table[iqr_filter]
        # Pass the question number and region and strata groupping to the
        # iqr_sum function.
        for i in range(0, len(iqrs_cols)):
            val_one = iqr_sum(filtered_iqr, move_cols[i])
            input_table[iqrs_cols[i]] = np.where(
                ((input_table["region"] == row[0]) & (input_table["strata"] == row[1])), val_one,
                input_table[iqrs_cols[i]])
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

    # df=pd.Series(df)
    dfSize = df.size
    import math
    # df=df.apply(unneg)
    if (dfSize % 2 == 0):

        sortedDF = df.sort_values()
        df = sortedDF.reset_index(drop=True)
        dfbottom = df[0:math.ceil(int(dfSize / 2))].median()
        dftop = df[math.ceil(int(dfSize / 2)):].median()
        iqr = dftop - dfbottom
        # iqr = quantile75 - quantile25
    else:
        sortedDF = df.sort_values()
        df = sortedDF.reset_index(drop=True)
        q1 = df[(math.ceil(0.25 * (dfSize + 1))) - 1]
        q3 = df[(math.floor(0.75 * (dfSize + 1))) - 1]
        iqr = q3 - q1

    return iqr