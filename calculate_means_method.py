import traceback
import json
import boto3
import pandas as pd
import os
import random


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
    try:

        # Clients
        sqs = boto3.client('sqs', region_name='eu-west-2')

        # ENV vars
        queue_url = os.environ['queue_url']
        current_period = os.environ['current_period']
        previous_period = os.environ['previous_period']
        questions_list = os.environ['questions_list']

        df = pd.DataFrame(event)
        
        df.groupby(['region', 'strata'])
        
        q_list = questions_list.split()
        
        # Filter new dataframe on questions
        movement_current = df.filter(['movement_Q601_asphalting_sand','movement_Q602_building_soft_sand','movement_Q603_concreting_sand','movement_Q604_bituminous_gravel','movement_Q605_concreting_gravel','movement_Q606_other_gravel','movement_Q607_constructional_fill'], axis=1)
        
        # Calculate question mean-movement values
        df['mean_'] = movement_current.mean(axis=1)
        # print(df['mean_'])
        
        final_output = df.to_json(orient='records')
        
    except Exception as exc:
        # purge = sqs.purge_queue(
        #   QueueUrl=queue_url
        # )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return final_output