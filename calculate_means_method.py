import logging

import pandas as pd

import imputation_functions as imp_func


def lambda_handler(event, context):
    """
    Generates an aggregated DataFrame containing the mean value for
    each of the period on period percentage movements, grouped by
    region and strata.
    :param event: JSON payload that contains: json_data, questions_list
                  Type: JSON.
    :param context: Context object
    :return: Success - {"success": True/False, "data"/"error": "JSON String"/"Message"}
    """
    current_module = "Means - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Means")

    try:
        logger.info("Means Method Begun")

        # Environment variables
        json_data = event["json_data"]
        distinct_values = event["distinct_values"]
        questions_list = event["questions_list"]

        movement_columns = imp_func.produce_columns("movement_", questions_list)

        logger.info("Validated params.")

        df = pd.DataFrame(json_data)

        logger.info("Succesfully retrieved data from event.")

        workingdf = df[movement_columns+distinct_values]

        counts = workingdf.groupby(distinct_values).count()
        # Rename columns to fit naming standards
        for column in movement_columns:
            counts.rename(
                columns={
                    column: column + "_count"
                },
                inplace=True,
            )

        # Create DataFrame which sums the movements grouped by region and strata
        sums = workingdf.groupby(distinct_values).sum()

        # Rename columns to fit naming standards
        for column in movement_columns:
            sums.rename(
                columns={
                    column: column + "_sum"
                },
                inplace=True,
            )

        counts = counts.reset_index(level=distinct_values)
        sums = sums.reset_index(level=distinct_values)
        moves = sums.merge(
            counts,
            left_on=distinct_values,
            right_on=distinct_values,
            how="left",
        )

        # Join on movements and counts on region & strata to DataFrame
        df = pd.merge(df, moves, on=distinct_values, how="left")

        for question in questions_list:
            df["mean_" + question] = df.apply(
                lambda x: x["movement_" + question + "_sum"]
                / x["movement_" + question + "_count"]
                if x["movement_" + question + "_count"] > 0 else 0,
                axis=1,
            )

        logger.info("Succesfully finished calculations of means.")

        final_output = {"data": df.to_json(orient="records")}

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
