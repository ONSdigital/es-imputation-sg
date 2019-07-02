import traceback
import json
import boto3
import pandas as pd
import os
import random

# Set up clients


# ENV vars

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
    """
       Generates an aggregated DataFrame containing the mean value for
       each of the period on period percentage movements, gropued by
       region and strata.
       :param event: Event object
       :param context: Context object
       :return: JSON string
       """
    lambda_client = boto3.client('lambda')

    movement_cols = os.environ['movement_columns']
    error_handler_arn = os.environ['error_handler_arn']
    queue_url = os.environ['queue_url']
    current_period = os.environ['current_period']
    previous_period = os.environ['previous_period']
    questions_list = os.environ['questions_list']

    try:
        df = pd.DataFrame(event)

        # for question in questions_list.split():
        #    df['movement_' + question + '_sum'] = df['movement_' + question].groupby(df['region']).transform('sum')

        workingdf = df[movement_cols.split(' ')]

        counts = workingdf.groupby(['region', 'strata']).count()
        # Rename columns to fit naming standards
        counts.rename(columns={'movement_Q601_asphalting_sand': 'movement_Q601_asphalting_sand_count',
                               'movement_Q602_building_soft_sand': 'movement_Q602_building_soft_sand_count',
                               'movement_Q603_concreting_sand': 'movement_Q603_concreting_sand_count',
                               'movement_Q604_bituminous_gravel': 'movement_Q604_bituminous_gravel_count',
                               'movement_Q605_concreting_gravel': 'movement_Q605_concreting_gravel_count',
                               'movement_Q606_other_gravel': 'movement_Q606_other_gravel_count',
                               'movement_Q607_constructional_fill': 'movement_Q607_constructional_fill_count'}
                      , inplace=True)

        # Create dataframe which sums the movements grouped by region and strata
        sums = workingdf.groupby(['region', 'strata']).sum()
        # Rename columns to fit naming standards
        sums.rename(columns={'movement_Q601_asphalting_sand': 'movement_Q601_asphalting_sand_sum',
                             'movement_Q602_building_soft_sand': 'movement_Q602_building_soft_sand_sum',
                             'movement_Q603_concreting_sand': 'movement_Q603_concreting_sand_sum',
                             'movement_Q604_bituminous_gravel': 'movement_Q604_bituminous_gravel_sum',
                             'movement_Q605_concreting_gravel': 'movement_Q605_concreting_gravel_sum',
                             'movement_Q606_other_gravel': 'movement_Q606_other_gravel_sum',
                             'movement_Q607_constructional_fill': 'movement_Q607_constructional_fill_sum'},
                    inplace=True)

        counts = counts.reset_index(level=['region', 'strata'])
        sums = sums.reset_index(level=['region', 'strata'])
        moves = sums.merge(counts, left_on=['region', 'strata'],
                           right_on=['region', 'strata'], how='left')

        ####join on movements and counts on region& strata to df
        df = pd.merge(df, moves, on=['region', 'strata'], how='left')

        for question in questions_list.split():
            df['mean_' + question] = df.apply(
                lambda x: x['movement_' + question + '_sum'] / x['movement_' + question + '_count'], axis=1)

        # df = df.groupby(['region', 'strata'])

        # q_list = questions_list.split()

        # Filter new dataframe on questions
        # movement_current = df.filter(['region','strata','movement_Q601_asphalting_sand','movement_Q602_building_soft_sand','movement_Q603_concreting_sand','movement_Q604_bituminous_gravel','movement_Q605_concreting_gravel','movement_Q606_other_gravel','movement_Q607_constructional_fill'], axis=1)

        # movement_current = movement_current.reset_index()

        # Calculate question mean-movement values
        # for question in q_list:
        #     df['mean_' + question] = movement_current.groupby(['region', 'strata']).mean()

        # counts = df.groupby(['region','strata'])[['movement_Q601_asphalting_sand','movement_Q602_building_soft_sand',
        #     'movement_Q603_concreting_sand','movement_Q604_bituminous_gravel',
        #     'movement_Q605_concreting_gravel','movement_Q606_other_gravel',
        #     'movement_Q607_constructional_fill']].count()

        # print("COUNTS")
        # print(list(counts))

        # sums = df.groupby(['region','strata'])[['movement_Q601_asphalting_sand','movement_Q602_building_soft_sand',
        #     'movement_Q603_concreting_sand','movement_Q604_bituminous_gravel',
        #     'movement_Q605_concreting_gravel','movement_Q606_other_gravel',
        #     'movement_Q607_constructional_fill']].sum()

        # print("SUMS")
        # print(list(sums))

        # merged = sums.merge(counts,left_on = ['region','strata'],right_on = ['region','strata'], how = 'left')
        # print("MERGED")
        # print(list(merged))

        # for question in questions_list.split():
        #     for i in range(3, 10):
        #         merged['mean_' + question] = merged.apply(lambda x: meanMove(x,i),axis= 1)

        # #print(df['mean_'])

        # final_df = df.merge(merged, left_on=['region', 'strata'])
        # print("FINAL_DF")
        # print(list(final_df))

        # final_output = final_df.to_json(orient='records')
        # print("FINAL_OUTPUT")
        # print(list(final_output))

    except Exception as exc:
        # purge = sqs.purge_queue(
        #   QueueUrl=queue_url
        # )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return df.to_json(orient='records')
