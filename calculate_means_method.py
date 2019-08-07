import os
import traceback

import marshmallow
import pandas as pd


class InputSchema(marshmallow.Schema):
    movement_cols = marshmallow.fields.Str(required=True)
    questions_list = marshmallow.fields.Str(required=True)


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """
    return "".join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def lambda_handler(event, context):
    """
    Generates an aggregated DataFrame containing the mean value for
    each of the period on period percentage movements, gropued by
    region and strata.
    :param event: Event object
    :param context: Context object
    :return: JSON string
    """
    # env vars
    config, errors = InputSchema().load(os.environ)
    if errors:
        raise ValueError(f"Error validating environment params: {errors}")

    try:
        df = pd.DataFrame(event)

        workingdf = df[config['movement_cols'].split(" ")]

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

        for question in config['questions_list'].split():
            df["mean_" + question] = df.apply(
                lambda x: x["movement_" + question + "_sum"]
                / x["movement_" + question + "_count"],
                axis=1,
            )

    except Exception as exc:
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc)),
        }

    return df.to_json(orient="records")
