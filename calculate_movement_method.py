import json
import logging

import boto3
import pandas as pd

import imputation_functions as imp_func

lambda_client = boto3.client('lambda', region_name='eu-west-2')
s3 = boto3.resource('s3')


def lambda_handler(event, context):
    """
    This method is responsible for creating the movements for each question and then
    recording them in the respective columns.
    :param event: JSON payload that contains: calculation_type, json_data, questions_list
                  Type: JSON.
    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Imputation Movement - Method"
    logger = logging.getLogger("Starting " + current_module)
    error_message = ''
    log_message = ''
    final_output = {}

    try:
        # Declare event vars
        calculation_type = event["calculation_type"]
        json_data = event["json_data"]
        questions_list = event["questions_list"]
        current_period = event['current_period']
        # Get relative calculation function
        calculation = getattr(imp_func, calculation_type)

        # Declared inside of lambda_handler so that tests work correctly on local.

        previous_period = calculate_adjacent_periods(current_period, "03")

        df = pd.DataFrame(json.loads(json_data))

        sorted_current = df[df.period == int(current_period)]
        sorted_previous = df[df.period == int(previous_period)]

        for question in questions_list.split(','):

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

        final_dataframe = sorted_current.append(sorted_previous, sort=False)

        filled_dataframe = final_dataframe.fillna(0.0)
        logger.info("Succesfully finished calculations of movement.")

        final_output = {"data": filled_dataframe.to_json(orient='records')}

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output


def calculate_adjacent_periods(current_period, periodicity):
    """
    Description: This method uses periodicity to calculate
    what should be the adjacent periods for a row,
    Then uses a filter to confirm whether these periods exist for a record.
    :param current_period: int/str(either) - The current period to find the previous for
    :param periodicity: String - The periodicity of the survey we are imputing for:
    01 = monthly, 02 = annually, 03 = quarterly
    :return: previous_period: String - The previous period.
    """
    monthly = "01"
    annually = "02"
    current_month = str(current_period)[4:]
    current_year = str(current_period)[:4]
    if periodicity == monthly:

        last_month = int(float(current_month)) - int(periodicity)
        last_year = int(current_year)
        if last_month < 1:
            last_year -= 1
            last_month += 12
        if last_month < 10:
            last_month = "0" + str(last_month)

        last_period = str(last_year) + str(last_month)

    elif periodicity == annually:

        last_period = str(int(current_period) - 1)

    else:  # quarterly(03)

        last_month = int(current_month) - 3
        last_year = int(current_year)
        if last_month < 1:
            last_year -= 1
            last_month += 4
        if len(str(last_month)) < 2:
            last_month = "0" + str(last_month)
        last_period = str(last_year) + str(last_month)

    return last_period
