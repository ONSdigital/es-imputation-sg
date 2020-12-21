import logging

import pandas as pd
from es_aws_functions import general_functions
from marshmallow import EXCLUDE, Schema, fields

import imputation_functions as imp_func


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    data = fields.List(fields.Dict, required=True)
    distinct_values = fields.List(fields.String, required=True)
    environment = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    survey = fields.Str(required=True)


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

    # Define run_id outside of try block
    run_id = 0

    # Set-up variables for status message
    bpm_queue_url = None

    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        distinct_values = runtime_variables["distinct_values"]
        environment = runtime_variables["environment"]
        json_data = runtime_variables["data"]
        questions_list = runtime_variables["questions_list"]
        survey = runtime_variables["survey"]

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module, run_id,
                                                           context=context)
        return {"success": False, "error": error_message}

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context)
        return {"success": False, "error": error_message}

    try:
        logger.info("Started - retrieved configuration variables.")

        movement_columns = imp_func.produce_columns("movement_", questions_list)

        df = pd.DataFrame(json_data)

        logger.info("Successfully retrieved data from event.")

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

        logger.info("Successfully finished calculations of means.")

        final_output = {"data": df.to_json(orient="records")}

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)
    final_output["success"] = True
    return final_output
