import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    environment = fields.Str(required=True)
    data = fields.List(fields.Dict, required=True)
    region_column = fields.Str(required=True)
    regionless_code = fields.Int(required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Adds a regionless / all-GB region code
    :param event: JSON payload that contains: json_data, region column and regionless code
                    - Type: JSON.
    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Add an all-GB regions - Method"
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
        environment = runtime_variables['environment']
        json_data = runtime_variables["data"]
        survey = runtime_variables['environment']
        regionless_code = runtime_variables["regionless_code"]
        region_column = runtime_variables["region_column"]

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

        # Get 2 copies of the data
        original_dataframe = pd.DataFrame(json_data)
        regionless_dataframe = pd.DataFrame(json_data)

        # Replace region in one of the sets
        regionless_dataframe[region_column] = regionless_code

        # Combine the original and region replaced data for output
        final_dataframe = pd.concat([original_dataframe, regionless_dataframe])

        final_output = {"data": final_dataframe.to_json(orient="records")}

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
