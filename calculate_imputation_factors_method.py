import logging

import pandas as pd

import imputation_functions as imp_func


def lambda_handler(event, context):
    """
    Calculates imputation factor for each question, in each aggregated group.
    :param event: JSON payload that contains: factors_type, json_data, questions_list
        - Type: JSON.
    :param context: lambda context
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Calculate Factors - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("CalculateFactors")
    logger.setLevel(10)
    try:
        logger.info("Calculate Factors Method Begun")

        # set up variables
        factors_parameters = event["factors_parameters"]["RuntimeVariables"]
        questions_list = event["questions_list"]
        df = pd.DataFrame(event["data_json"])

        # Get relative calculation function
        calculation = getattr(imp_func, factors_parameters["factors_type"])

        # Some surveys will need to use the regional mean, extract it ahead of time
        if "regional_mean" in factors_parameters:
            gb_row = df.loc[df[factors_parameters["region_column"]] ==
                            factors_parameters["regionless_code"]].head(1)

        for question in questions_list.split(","):
            # For surveys that user regional mean, extract it for this question
            if "regional_mean" in factors_parameters:
                factors_parameters[factors_parameters["regional_mean"]] =\
                    gb_row["mean_" + question]

            df = df.apply(lambda x: calculation(x, question, factors_parameters), axis=1)

            logger.info("Calculated Factors for " + str(question))
        factors_dataframe = df

        logger.info("Succesfully finished calculations of factors")

        final_output = {"data": factors_dataframe.to_json(orient="records")}

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
    final_output["success"] = True
    return final_output
