import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields


class SumSchema(Schema):
    column_name = fields.Str(required=True)
    data = fields.Dict(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    data = fields.List(fields.Dict, required=True)
    environment = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    sum_columns = fields.Nested(SumSchema, many=True, required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Applies imputation factors on a question-by-question basis.
    :param event:  JSON payload that contains: json_data and questions_list - Type: JSON.
    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Apply Factors - Method"
    error_message = ""

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
        environment = runtime_variables["environment"]
        json_data = runtime_variables["data"]
        questions_list = runtime_variables["questions_list"]
        sum_columns = runtime_variables["sum_columns"]
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

        working_dataframe = pd.DataFrame(json_data)

        for question in questions_list:
            # Loop through each question value, impute based on factor and previous value
            # then drop the previous value and the imp factor
            working_dataframe[question] = working_dataframe.apply(
                lambda x:
                general_functions.sas_round(x["prev_" + question] *
                                            x["imputation_factor_" + question]),
                axis=1,
            )

            logger.info("Completed imputation of " + str(question))

        working_dataframe = working_dataframe.apply(
            lambda x: sum_data_columns(x, sum_columns), axis=1)

        final_output = {"data": working_dataframe.to_json(orient="records")}

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


def sum_data_columns(input_row, sum_columns):
    # Calculate all sum columns.
    for sum_column in sum_columns:
        new_sum = 0
        for data_column in sum_column["data"]:
            if sum_column["data"][data_column] == "+":
                new_sum += input_row[data_column]
            elif sum_column["data"][data_column] == "-":
                new_sum -= input_row[data_column]
        input_row[sum_column["column_name"]] = int(new_sum)

    return input_row
