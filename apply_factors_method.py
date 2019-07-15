
import traceback
import pandas as pd


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
    try:
        working_dataframe = pd.DataFrame(event)

        question_columns = ['Q601_asphalting_sand', 'Q602_building_soft_sand', 'Q603_concreting_sand', 'Q604_bituminous_gravel',
                            'Q605_concreting_gravel', 'Q606_other_gravel', 'Q607_constructional_fill']

        for question in question_columns:
            # Loop through each question value, impute based on factor and previous value
            # then drop the previous value and the imp factor
            working_dataframe[question] = working_dataframe.apply(lambda x: x['prev_'+question]*x['imputation_factor_'+question], axis=1)
            working_dataframe = working_dataframe.drop(['prev_'+question, 'imputation_factor_'+question], axis=1)

    except Exception as exc:
        print(exc)
        import boto3
        import os
        queue_url = os.environ['queue_url']
        sqs = boto3.client('sqs', region_name='eu-west-2')
        ### COMMENTED OUT FOR TESTING ###
        purge = sqs.purge_queue(
             QueueUrl=queue_url
        )

        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }

    return working_dataframe.to_json(orient='records')
