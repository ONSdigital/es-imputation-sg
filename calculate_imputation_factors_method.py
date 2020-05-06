import logging
##### Come back here later move factors params to the runtime setup!!! Wrangler Too.
import pandas as pd
from es_aws_functions import general_functions

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
    logger = logging.getLogger("CalculateFactors")
    logger.setLevel(10)
    run_id = 0
    try:
        logger.info("Calculate Factors Method Begun")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Factors Parameters Class? Or In Wrangler? ££££££££££££££££££££££££££££££££££££££
        runtime_variables, errors = RuntimeSchema().load(event["RuntimeVariables"])
        if errors:
            logger.error(f"Error validating runtime params: {errors}")
            raise ValueError(f"Error validating runtime params: {errors}")

        logger.info("Validated parameters.")

        # Runtime Variables
        factors_parameters = runtime_variables[
            "factors_parameters"]["RuntimeVariables"]
        questions_list = runtime_variables["questions_list"]
        distinct_values = runtime_variables["distinct_values"]
        df = pd.DataFrame(runtime_variables["data"])
        survey_column = factors_parameters["survey_column"]

        logger.info("Retrieved configuration variables.")

        # Get relative calculation function
        calculation = getattr(imp_func, factors_parameters["factors_type"])

        # Pass the distinct values to the factors function in its parameters
        factors_parameters["distinct_values"] = distinct_values

        # Some surveys will need to use the regional mean, extract them ahead of time
        if "regional_mean" in factors_parameters:
            # split to get only regionless data
            gb_rows = df.loc[df[factors_parameters["region_column"]] ==
                             factors_parameters["regionless_code"]]
            # produce column names
            means_columns = imp_func.produce_columns("mean_", questions_list)
            counts_columns = imp_func.\
                produce_columns("movement_", questions_list, suffix="_count")
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

        logger.info("Successfully finished calculations of factors")

        final_output = {"data": factors_dataframe.to_json(orient="records")}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output
