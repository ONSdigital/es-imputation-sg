import json
import logging
import os

import boto3
import pandas as pd
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields
from marshmallow.validate import Equal

from imputation_functions import produce_columns


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    bucket_name = fields.Str(required=True)
    method_name = fields.Str(required=True)
    response_type = fields.Str(required=True)
    run_environment = fields.Str(required=True)


class FactorsSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    region_column = fields.Str(required=True)
    regionless_code = fields.Int(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    bpm_queue_url = fields.Str(required=True)
    current_data = fields.Str(required=True)
    distinct_values = fields.List(fields.String, required=True)
    factors_parameters = fields.Dict(
        keys=fields.String(validate=Equal(comparable="RuntimeVariables")),
        values=fields.Nested(FactorsSchema, required=True))
    in_file_name = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    previous_data = fields.Str(required=True)
    questions_list = fields.List(fields.String, required=True)
    unique_identifier = fields.List(fields.String, required=True)
    sns_topic_arn = fields.Str(required=True)
    sum_columns = fields.List(fields.Dict, required=True)
    total_steps = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the apply factors statistical method.
    The method requires a column per question to store the factors.
    :param event:  Contains all the variables which are required for the specific run.
    :param context: N/A
    :return: Success & None/Error - Type: JSON
    """
    current_module = "Imputation Apply Factors - Wrangler."
    error_message = ""
    logger = general_functions.get_logger()
    # Define run_id outside of try block

    bpm_queue_url = None
    current_step_num = "4"

    run_id = 0
    try:
        logger.info("Starting " + current_module)
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event["RuntimeVariables"]["run_id"]

        # Set up clients
        lambda_client = boto3.client("lambda", region_name="eu-west-2")

        environment_variables = EnvironmentSchema().load(os.environ)

        runtime_variables = RuntimeSchema().load(event["RuntimeVariables"])

        logger.info("Validated parameters.")

        # Environment Variables
        bucket_name = environment_variables["bucket_name"]
        method_name = environment_variables["method_name"]
        response_type = environment_variables["response_type"]
        run_environment = environment_variables["run_environment"]

        # Runtime Variables
        bpm_queue_url = runtime_variables["bpm_queue_url"]
        current_data = runtime_variables["current_data"]
        distinct_values = runtime_variables["distinct_values"]
        factors_parameters = runtime_variables["factors_parameters"]["RuntimeVariables"]
        in_file_name = runtime_variables["in_file_name"]
        out_file_name = runtime_variables["out_file_name"]
        previous_data = runtime_variables["previous_data"]
        questions_list = runtime_variables["questions_list"]
        reference = runtime_variables["unique_identifier"][0]
        region_column = factors_parameters["region_column"]
        regionless_code = factors_parameters["regionless_code"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        sum_columns = runtime_variables["sum_columns"]
        total_steps = runtime_variables["total_steps"]

        logger.info("Retrieved configuration variables.")

        # Get factors data from calculate_factors
        factors_dataframe = aws_functions.read_dataframe_from_s3(
            bucket_name, in_file_name)
        logger.info("Successfully retrieved factors data from s3")

        # Get data from module that preceded imputation
        input_data = aws_functions.read_dataframe_from_s3(bucket_name, current_data)

        # Split out non responder data from input
        non_responder_dataframe = input_data[input_data[response_type] == 1]
        logger.info("Successfully retrieved raw-input data from s3")

        # Read in previous period data for current period non-responders
        prev_period_data = aws_functions.read_dataframe_from_s3(bucket_name,
                                                                previous_data)
        logger.info("Successfully retrieved previous period data from s3")
        # Filter so we only have those that responded in prev
        prev_period_data = prev_period_data[prev_period_data[response_type] == 2]

        prev_questions_list = produce_columns(
            "prev_",
            questions_list,
            [reference]
        )

        for question in questions_list:
            prev_period_data = prev_period_data.rename(
                index=str, columns={question: "prev_" + question}
            )
        logger.info("Successfully renamed previous period data")

        non_responder_dataframe_with_prev = pd.merge(
            non_responder_dataframe,
            prev_period_data[prev_questions_list],
            on=reference,
        )
        logger.info("Successfully merged previous period data with non-responder df")

        # Merge the factors onto the non responders
        non_responders_with_factors = pd.merge(
            non_responder_dataframe_with_prev,
            factors_dataframe[
                produce_columns(
                    "imputation_factor_",
                    questions_list,
                    distinct_values
                )
            ],
            on=distinct_values,
            how="inner",
        )
        logger.info("Successfully merged non-responders with factors")

        # Collects all rows where an imputation factor doesn't exist.
        dropped_rows = non_responder_dataframe_with_prev[
            ~non_responder_dataframe_with_prev[reference].isin(
                non_responders_with_factors[reference])].dropna()

        if len(dropped_rows) > 0:
            merge_values = distinct_values
            merge_values.remove(region_column)

            # Collect the GB region imputation factors if they exist.
            regionless_factors = \
                factors_dataframe[
                    produce_columns("imputation_factor_",
                                    questions_list,
                                    distinct_values)
                ][factors_dataframe[region_column] == regionless_code]

            if len(merge_values) != 0:
                # Basic merge where we have values to merge on.
                dropped_rows_with_factors = \
                    pd.merge(dropped_rows, regionless_factors,
                             on=merge_values, how="inner")
            else:
                # Added a column to both dataframes to use for the merge.
                dropped_rows["Temp_Key"] = 0
                regionless_factors["Temp_Key"] = 0

                dropped_rows_with_factors = \
                    pd.merge(dropped_rows, regionless_factors,
                             on="Temp_Key", how="inner")

                dropped_rows_with_factors = dropped_rows_with_factors.drop("Temp_Key",
                                                                           axis=1)

            non_responders_with_factors = \
                pd.concat([non_responders_with_factors, dropped_rows_with_factors])
            logger.info("Successfully merged missing rows with non_responders")

        payload = {
            "RuntimeVariables": {
                "bpm_queue_url": bpm_queue_url,
                "data": json.loads(
                    non_responders_with_factors.to_json(orient="records")),
                "questions_list": questions_list,
                "sum_columns": sum_columns,
                "run_id": run_id
            }
        }

        # Non responder data should now contain all previous values
        #   and the imputation columns
        imputed_data = lambda_client.invoke(
            FunctionName=method_name,
            Payload=json.dumps(payload),
        )
        logger.info("Successfully invoked method.")

        json_response = json.loads(imputed_data.get("Payload").read().decode("UTF-8"))
        logger.info("JSON extracted from method response.")

        if not json_response["success"]:
            raise exception_classes.MethodFailure(json_response["error"])

        imputed_non_responders = pd.read_json(json_response["data"], dtype=False)

        # retrieve current responders from input data..
        current_responders = input_data[
            input_data[response_type] == 2
            ]

        # Joining Datasets Together.
        final_imputed = pd.concat([current_responders, imputed_non_responders])
        logger.info("Successfully joined imputed data with responder data")

        # Create A List Of Factor Columns To Drop
        cols_to_drop = produce_columns("imputation_factor_", questions_list,
                                       produce_columns("prev_", questions_list))

        filtered_data = final_imputed.drop(cols_to_drop, axis=1)

        message = filtered_data.to_json(orient="records")

        aws_functions.save_to_s3(bucket_name, out_file_name, message)
        logger.info("Successfully sent data to s3.")

        if run_environment != "development":
            logger.info(aws_functions.delete_data(bucket_name, current_data))
            logger.info(aws_functions.delete_data(bucket_name, previous_data))
            logger.info(aws_functions.delete_data(bucket_name, in_file_name))
            logger.info("Successfully deleted input data.")

        aws_functions.send_sns_message(sns_topic_arn, "Imputation - Apply Factors.")
        logger.info("Successfully sent message to sns.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context=context,
                                                           bpm_queue_url=bpm_queue_url)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    # Send end status to BPM.
    status = "DONE"
    aws_functions.send_bpm_status(bpm_queue_url, current_module, status, run_id,
                                  current_step_num, total_steps)

    return {"success": True}
