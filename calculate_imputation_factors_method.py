import traceback
import os
import pandas as pd
import boto3


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


def get_environment_variable(variable):
    """
    obtains the environment variables and tests collection.
    :param variable:
    :return: output = varaible name
    """
    output = os.environ.get(variable, None)
    if output is None:
        raise ValueError(str(variable)+" config parameter missing.")
    return output


def lambda_handler(event, context):
    """
    Calculates the imputation factors, called by the Calculate imputation factors wrangler.

    :param event: lambda event
    :param context: lambda context
    :return: json dataset
    """

    # set up clients
    sqs = boto3.client('sqs')

    try:
        # set up variables
        questions = get_environment_variable('questions')
        first_threshold = get_environment_variable('first_threshold')
        second_threshold = get_environment_variable('second_threshold')
        third_threshold = get_environment_variable('third_threshold')
        first_imputation_factor = get_environment_variable('first_imputation_factor')
        second_imputation_factor = get_environment_variable('second_imputation_factor')
        third_imputation_factor = get_environment_variable('third_imputation_factor')

        df = pd.DataFrame(event)

        def calculate_imputation_factors(row, question):
            """
            Calculates the imputation factors for the DataFrame on row by row basis.
            - Calculates imputation factor for each question, in each aggregated group, by:
                Region
                Land or Marine
                Count of refs within cell

            :param row: row of DataFrame
            :param question: question
            :return: row of DataFrame
            """
            if row['region'] == 14:
                if row['land_or_marine'] == 'L':
                    if row['movement_' + question + '_count'] < int(first_threshold):
                        row['imputation_factor_' + question] = int(first_imputation_factor)
                    else:
                        row['imputation_factor_' + question] = row['mean_' + question]
                else:
                    if row['movement_' + question + '_count'] < int(second_threshold):
                        row['imputation_factor_' + question] = int(second_imputation_factor)
                    else:
                        row['imputation_factor_' + question] = row['mean_' + question]
            else:
                if row['movement_' + question + '_count'] < int(third_threshold):
                    row['imputation_factor_' + question] = int(third_imputation_factor)
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
