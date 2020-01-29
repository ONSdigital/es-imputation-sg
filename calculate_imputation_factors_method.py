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
        distinct_values = event["distinct_values"]
        df = pd.DataFrame(event["data_json"])
        survey_column = factors_parameters['survey_column']
        # Get relative calculation function
        calculation = getattr(imp_func, factors_parameters["factors_type"])

        # Pass the distinct values to the factors function in its parameters
        factors_parameters['distinct_values'] = distinct_values

        # Some surveys will need to use the regional mean, extract them ahead of time
        if "regional_mean" in factors_parameters:
            # split to get only regionless data
            gb_rows = df.loc[df[factors_parameters["region_column"]] ==
                             factors_parameters["regionless_code"]]
            # produce column names
            means_columns = imp_func.produce_columns("mean_", questions_list)
            counts_columns = imp_func.\
                produce_columns("movement_", questions_list, suffix='_count')
            gb_columns = \
                means_columns +\
                counts_columns +\
                distinct_values +\
                [survey_column]

            factor_columns = imp_func.\
                produce_columns("imputation_factor_",
                                questions_list,
                                distinct_values+[survey_column])

            # select only gb columns and then drop duplicates, leaving one row per strata
            gb_rows = gb_rows[gb_columns].drop_duplicates()
            factors_parameters[factors_parameters["regional_mean"]] = ""

            # calculate gb factors ahead of time
            gb_rows = gb_rows.apply(
                lambda x: calculation(x, questions_list, factors_parameters), axis=1)

            # reduce gb_rows to distinct_values, survey, and the factors
            gb_factors = \
                gb_rows[factor_columns]

            # add gb_factors to factors parameters to send to calculation
            factors_parameters[factors_parameters["regional_mean"]] = \
                gb_factors

        df = df.apply(lambda x: calculation(x, questions_list, factors_parameters),
                      axis=1)
        logger.info("Calculated Factors for " + str(questions_list))

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
