import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields

import imputation_functions as imp_func


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    current_period = fields.Str(required=True)
    data = fields.List(fields.Dict, required=True)
    environment = fields.Str(required=True)
    movement_type = fields.Str(required=True)
    period_column = fields.Str(required=True)
    previous_period = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    survey = fields.Str(required=True)


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
    error_message = ""
    final_output = {}

    # Define run_id outside of try block
    run_id = 0

    # Set-up variables for status message
    bpm_queue_url = None

    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        current_period = runtime_variables["current_period"]
        environment = runtime_variables["environment"]
        json_data = runtime_variables["data"]
        movement_type = runtime_variables["movement_type"]
        period_column = runtime_variables["period_column"]
        previous_period = runtime_variables["previous_period"]
        questions_list = runtime_variables["questions_list"]
        survey = runtime_variables["survey"]

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id,
                                                           context=context)
        return {"success": False, "error": error_message}

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context)
        return {"success": False, "error": error_message}

    try:
        logger.info("Started - retrieved configuration variables.")

        # Get relative calculation function
        calculation = getattr(imp_func, movement_type)

        df = pd.DataFrame(json_data)

        sorted_current = df[df[period_column].astype("str") == str(current_period)].copy()
        sorted_previous = df[df[period_column].astype("str") == str(previous_period)]

        for question in questions_list:

            # Converted to list due to issues with Numpy dtypes and math operations.
            current_list = sorted_current[question].tolist()
            previous_list = sorted_previous[question].tolist()

            result_list = []

            # len is used so the correct amount of iterations for the loop.
            for i in range(0, len(sorted_current)):

                # This check is too prevent the DivdebyZeroError.
                if previous_list[i] != 0:
                    number = calculation(current_list[i], previous_list[i])
                else:
                    number = 0.0

                result_list.append(number)

            sorted_current["movement_" + question] = result_list

        filled_dataframe = sorted_current.fillna(0.0)
        logger.info("Successfully finished calculations of movement.")

        final_output = {"data": filled_dataframe.to_json(orient="records")}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output
