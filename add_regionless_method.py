import logging

import pandas as pd


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
    log_message = ""
    logger = logging.getLogger("AllGB")
    logger.setLevel(10)
    try:
        logger.info("Starting " + current_module)

        # Get envrionment variables
        json_data = event["json_data"]
        regionless_code = event["regionless_code"]
        region_column = event["region_column"]

        # Get 2 copies of the data
        original_dataframe = pd.DataFrame(json_data)
        regionless_dataframe = pd.DataFrame(json_data)

        # Replace region in one of the sets
        regionless_dataframe[region_column] = regionless_code

        # Combine the original and region replaced data for output
        final_dataframe = pd.concat([original_dataframe, regionless_dataframe])

        final_output = {"data": final_dataframe.to_json(orient="records")}

    except KeyError as e:
        error_message = (
            "Key Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = (
            "General Error in "
            + current_module
            + " ("
            + str(type(e))
            + ") |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output['success'] = True
    print(final_output)
    return final_output
