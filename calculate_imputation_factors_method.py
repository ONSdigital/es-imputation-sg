import traceback
import os
import pandas as pd
import boto3

# set up clients
sns = boto3.client('sns')
lambda_client = boto3.client('lambda')


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
    Calculates the imputation factors, called by the Calculate imputation factors wrangler.

    :param event: lambda event
    :param context: lambda context
    :return: json dataset
    """
    try:
        # set up variables
        questions = os.environ['questions']
        first_threshold = os.environ['first_threshold']
        second_threshold = os.environ['second_threshold']
        third_threshold = os.environ['third_threshold']
        first_imputation_factor = os.environ['first_imputation_factor']
        second_imputation_factor = os.environ['second_imputation_factor']
        third_imputation_factor = os.environ['third_imputation_factor']

        df = pd.DataFrame(event)

        def calculate_imputation_factors(row, question):
            """
            Calculates the imputation factors for the DataFrame on row by row basis.
            - Calculates imputation factor for each question, in each aggregated group, determined by:
                Region
                Land or Marine
                Count of refs within cell

            :param row: row of DataFrame
            :param question: question
            :return: row of DataFrame
            """
            if row['region'] == 14:
                if row['land_or_marine'] == 'L':
                    if row['movement_' + question + '_count'] < first_threshold:
                        row['imputation_factor_' + question] = first_imputation_factor
                    else:
                        row['imputation_factor_' + question] = row['mean_' + question]
                else:
                    if row['movement_' + question + '_count'] < second_threshold:
                        row['imputation_factor_' + question] = second_imputation_factor
                    else:
                        row['imputation_factor_' + question] = row['mean_' + question]
            else:
                if row['movement_' + question + '_count'] < third_threshold:
                    row['imputation_factor_' + question] = third_imputation_factor
                else:
                    row['imputation_factor_' + question] = row['mean_' + question]

            return row

        for question in questions.split(' '):
            df = df.apply(lambda x: calculate_imputation_factors(x, question), axis=1)
        factors_dataframe = df

    except Exception as exc:
        # purge = sqs.purge_queue(
        #  QueueUrl=queue_url
        # )

        return {
            "success": False,
            "error": "Unexpected method exception {}".format(_get_traceback(exc))
        }

    final_output = factors_dataframe.to_json(orient='records')

    return final_output
