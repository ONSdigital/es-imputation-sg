import logging

import pandas as pd


def lambda_handler(event, context):
    """
    Applies imputation factors on a question-by-question basis.
    :param event:  JSON payload that contains: json_data and questions_list - Type: JSON.
    :param context: N/A
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Apply Factors - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Apply")
    logger.setLevel(10)
    try:
        logger.info("Apply Factors Method Begun")

        json_data = event["json_data"]
        working_dataframe = pd.DataFrame(json_data)

        questions_list = event["questions_list"]

        for question in questions_list:
            # Loop through each question value, impute based on factor and previous value
            # then drop the previous value and the imp factor
            working_dataframe[question] = working_dataframe.apply(
                lambda x: x["prev_" + question] * x["imputation_factor_" + question],
                axis=1,
            )
            working_dataframe = working_dataframe.drop(
                ["prev_" + question, "imputation_factor_" + question], axis=1
            )
            logger.info("Completed imputation of " + str(question))

        final_output = {"data": working_dataframe.to_json(orient="records")}

    except TypeError as e:
        error_message = (
            "Bad Data type in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except ValueError as e:
        error_message = (
            "Input Error in "
            + current_module
            + " |- "
            + str(e.args)
            + " | Request ID: "
            + str(context.aws_request_id)
        )
        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
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
    return final_output
