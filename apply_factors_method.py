import logging

import pandas as pd


def lambda_handler(event, context):
    """
    Calculate and applies imputation factors on a question-by-question basis.
    :param event: N/A
    :param context: N/A
    :return: JSON - String
    """
    current_module = "Apply Factors - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Apply")
    logger.setLevel(10)
    try:
        logger.info("Apply Factors Method Begun")
        working_dataframe = pd.DataFrame(event)

        question_columns = [
            "Q601_asphalting_sand",
            "Q602_building_soft_sand",
            "Q603_concreting_sand",
            "Q604_bituminous_gravel",
            "Q605_concreting_gravel",
            "Q606_other_gravel",
            "Q607_constructional_fill",
        ]

        for question in question_columns:
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
    return working_dataframe.to_json(orient="records")
