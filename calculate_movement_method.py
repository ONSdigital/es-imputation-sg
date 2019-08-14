import os

import boto3
import pandas as pd
import logging
from marshmallow import Schema, fields

lambda_client = boto3.client('lambda', region_name='eu-west-2')
s3 = boto3.resource('s3')


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    current_period = fields.Str(required=True)
    previous_period = fields.Str(required=True)
    questions_list = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This method is responsible for creating the movements for each question and then
    recording them in the respective columns.
    :param event: The data in which you are calculating the movements on, this requires
                  the current and previous period data - Type: JSON.
    :param context: N/A
    :return: final_output: The input data but now with the correct movements for
                           the respective question columns - Type: JSON.
    """
    current_module = "Imputation Movement - Method"
    logger = logging.getLogger("Starting " + current_module)
    error_message = ''
    log_message = ''
    final_output = {}

    try:

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Declared inside of lambda_handler so that tests work correctly on local.
        current_period = config['current_period']
        previous_period = config['previous_period']
        questions_list = config['questions_list']

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
                if previous_list[i] != 0:
                    number = (current_list[i] - previous_list[i]) / previous_list[i]
                else:
                    number = 0.0

                result_list.append(number)

            sorted_current['movement_' + question] = result_list

        final_dataframe = sorted_current.append(sorted_previous, sort=False)

        filled_dataframe = final_dataframe.fillna(0.0)

        final_output = filled_dataframe.to_json(orient='records')

    except ValueError as e:
        error_message = "Parameter validation error" \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context['aws_request_id'])

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)

    return final_output
