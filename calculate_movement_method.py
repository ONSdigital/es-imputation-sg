import traceback
import boto3
import os
import pandas as pd

lambda_client = boto3.client('lambda')
s3 = boto3.resource('s3')


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
    This method is responsible for creating the movements for each question and then recording them
    in the respective columns.
    :param event: The data in which you are calculating the movements on, this requires the current
                  and previous period data - Type: JSON
    :param context: N/A
    :return: final_output: The input data but now with the correct movements for the respective
                           question columns - Type: JSON
    """
    try:

        # Declared inside of lambda_handler so that tests work correctly on local.
        current_period = os.environ['current_period']
        previous_period = os.environ['previous_period']
        questions_list = os.environ['questions_list']

        df = pd.DataFrame(event)

        sorted_current = df[df.period == int(current_period)]
        sorted_previous = df[df.period == int(previous_period)]

        for question in questions_list.split():

            # Converted to list due to issues with Numpy dtypes and math operations.
            current_list = sorted_current[question].tolist()
            previous_list = sorted_previous[question].tolist()

            result_list = []
            # .Length is used so the correct amount of iterations for the loop.
            for i in range(0, len(sorted_current)):

                # This check is too prevent the DivdebyZeroError.
                if current_list[i] and previous_list[i] != 0:
                    number = (current_list[i] - previous_list[i]) / current_list[i]
                else:
                    number = 0.0

                result_list.append(number)

            sorted_current['movement_' + question] = result_list

        final_dataframe = sorted_current.append(sorted_previous, sort=False)

        filled_dataframe = final_dataframe.fillna(0.0)

        final_output = filled_dataframe.to_json(orient='records')

    except Exception as exc:

        return {
            "success": False,
            "error": "Unexpected method exception {}".format(_get_traceback(exc))
        }

    return final_output
