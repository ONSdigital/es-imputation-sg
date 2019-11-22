import logging
import os

import pandas as pd
from marshmallow import Schema, fields


class InputSchema(Schema):
    movement_columns = fields.Str(required=True)
    questions_list = fields.Str(required=True)


def lambda_handler(event, context):
    """
    Generates an aggregated DataFrame containing the mean value for
    each of the period on period percentage movements, grouped by
    region and strata.
    :param event: Event object
    :param context: Context object
    :return: JSON string
    """
    current_module = "Means - Method"
    error_message = ""
    log_message = ""
    logger = logging.getLogger("Means")

    try:

        logger.info("Means Method Begun")

        # env vars
        config, errors = InputSchema().load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Env vars
        json_data = event["json_data"]
        distinct_values = event["distinct_values"]

        movement_columns = config['movement_columns'].split(',')
        questions_list = config['questions_list'].split(',')

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

        # Create dataframe which sums the movements grouped by region and strata
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

        # join on movements and counts on region& strata to df
        df = pd.merge(df, moves, on=distinct_values, how="left")

        for question in questions_list:
            df["mean_" + question] = df.apply(
                lambda x: x["movement_" + question + "_sum"]
                / x["movement_" + question + "_count"]
                if x["movement_" + question + "_count"] > 0 else 0,
                axis=1,
            )

        logger.info("Succesfully calculated means.")

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
        else:
            logger.info("Successfully completed module: " + current_module)
            return df.to_json(orient="records")
