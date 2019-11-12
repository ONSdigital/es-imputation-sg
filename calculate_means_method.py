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

        logger.info("Validated params.")

        df = pd.DataFrame(event)

        logger.info("Succesfully retrieved data from event.")

        workingdf = df[config['movement_columns'].split(",")]

        counts = workingdf.groupby(["region", "strata"]).count()
        # Rename columns to fit naming standards
        counts.rename(
            columns={
                "movement_Q601_asphalting_sand": "movement_Q601_asphalting_sand_count",
                "movement_Q602_building_soft_sand": "movement_Q602_building_soft_sand_count",  # noqa: E501
                "movement_Q603_concreting_sand": "movement_Q603_concreting_sand_count",
                "movement_Q604_bituminous_gravel": "movement_Q604_bituminous_gravel_count",  # noqa: E501
                "movement_Q605_concreting_gravel": "movement_Q605_concreting_gravel_count",  # noqa: E501
                "movement_Q606_other_gravel": "movement_Q606_other_gravel_count",
                "movement_Q607_constructional_fill": "movement_Q607_constructional_fill_count",  # noqa: E501
            },
            inplace=True,
        )

        # Create dataframe which sums the movements grouped by region and strata
        sums = workingdf.groupby(["region", "strata"]).sum()
        # Rename columns to fit naming standards
        sums.rename(
            columns={
                "movement_Q601_asphalting_sand": "movement_Q601_asphalting_sand_sum",
                "movement_Q602_building_soft_sand": "movement_Q602_building_soft_sand_sum",  # noqa: E501
                "movement_Q603_concreting_sand": "movement_Q603_concreting_sand_sum",
                "movement_Q604_bituminous_gravel": "movement_Q604_bituminous_gravel_sum",  # noqa: E501
                "movement_Q605_concreting_gravel": "movement_Q605_concreting_gravel_sum",  # noqa: E501
                "movement_Q606_other_gravel": "movement_Q606_other_gravel_sum",
                "movement_Q607_constructional_fill": "movement_Q607_constructional_fill_sum",  # noqa: E501
            },
            inplace=True,
        )

        counts = counts.reset_index(level=["region", "strata"])
        sums = sums.reset_index(level=["region", "strata"])
        moves = sums.merge(
            counts,
            left_on=["region", "strata"],
            right_on=["region", "strata"],
            how="left",
        )

        # join on movements and counts on region& strata to df
        df = pd.merge(df, moves, on=["region", "strata"], how="left")

        for question in config['questions_list'].split(','):
            df["mean_" + question] = df.apply(
                lambda x: x["movement_" + question + "_sum"]
                / x["movement_" + question + "_count"],
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
