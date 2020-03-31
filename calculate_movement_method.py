import json
import logging

import boto3
import pandas as pd
from es_aws_functions import general_functions

import imputation_functions as imp_func

lambda_client = boto3.client('lambda', region_name='eu-west-2')
s3 = boto3.resource('s3')


def lambda_handler(event, context):
    """
    This method is responsible for creating the movements for each question and then
    recording them in the respective columns.
    :param event: JSON payload that contains: movement_type, json_data, questions_list
                  Type: JSON.
    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Imputation Movement - Method"
    logger = logging.getLogger("Starting " + current_module)
    error_message = ''
    final_output = {}
    run_id = 0
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['RuntimeVariables']['run_id']
        # Declare event vars
        movement_type = event['RuntimeVariables']["movement_type"]
        json_data = event['RuntimeVariables']["json_data"]
        questions_list = event['RuntimeVariables']["questions_list"]
        current_period = event['RuntimeVariables']['current_period']
        period_column = event['RuntimeVariables']['period_column']
        previous_period = event['RuntimeVariables']['previous_period']

        # Get relative calculation function
        calculation = getattr(imp_func, movement_type)

        df = pd.DataFrame(json_data)

        sorted_current = df[df[period_column].astype('str') == str(current_period)]
        sorted_previous = df[df[period_column].astype('str') == str(previous_period)]

        for question in questions_list:

            # Converted to list due to issues with Numpy dtypes and math operations.
            current_list = sorted_current[question].tolist()
            previous_list = sorted_previous[question].tolist()

            result_list = []

            # .len is used so the correct amount of iterations for the loop.
            for i in range(0, len(sorted_current)):

                # This check is too prevent the DivdebyZeroError.
                if previous_list[i] != 0:
                    number = calculation(current_list[i], previous_list[i])
                else:
                    number = 0.0

                result_list.append(number)

            sorted_current['movement_' + question] = result_list

        filled_dataframe = sorted_current.fillna(0.0)
        logger.info("Successfully finished calculations of movement.")

        final_output = {"data": filled_dataframe.to_json(orient='records')}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    return final_output
