import json
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError, IncompleteReadError
from esawsfunctions import funk
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    :return: None
    """
    checkpoint = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    current_segmentation = fields.Str(required=True)
    current_time = fields.Str(required=True)
    in_file_name = fields.Str(required=True)
    incoming_message_group = fields.Str(required=True)
    method_name = fields.Str(required=True)
    non_response_file = fields.Str(required=True)
    out_file_name = fields.Str(required=True)
    previous_period_file = fields.Str(required=True)
    previous_segmentation = fields.Str(required=True)
    previous_time = fields.Str(required=True)
    questions_list = fields.Str(required=True)
    reference = fields.Str(required=True)
    response_type = fields.Str(required=True)
    segmentation = fields.Str(required=True)
    sns_topic_arn = fields.Str(required=True)
    sqs_message_group_id = fields.Str(required=True)
    sqs_queue_url = fields.Str(required=True)
    stored_segmentation = fields.Str(required=True)
    time = fields.Str(required=True)


def strata_mismatch_detector(data, current_period, time, reference, segmentation,
                             stored_segmentation, current_time, previous_time,
                             current_segmentation, previous_segmentation):
    """
    Looks only at id and strata columns. Then drops any duplicated rows(keep=false means
    that if there is a dupe it'll drop both). If there are any rows in this DataFrame it
    shows that the reference-strata combination was unique, and therefore the strata is
    different between periods.
    :param data: The data the miss-match detection will be performed on.
    :param current_period: The current period of the run.
    :param time: Field name which is used as a gauge of time'. Added for IAC config.
    :param reference: Field name which is used as a reference for IAC.
    :param segmentation: Field name of the segmentation used for IAC.
    :param stored_segmentation: Field name of stored segmentation for IAC.
    :param current_time: Field name of the current time used for IAC.
    :param previous_time: Field name of the previous time used for IAC.
    :param current_segmentation: Field name of the current segmentation used for IAC.
    :param previous_segmentation: Field name of the current segmentation used for IAC.
    :return: data - Type: DataFrame, data_anomalies - Type: DataFrame
    """
    data_anomalies = data[[reference, segmentation, time]]

    data_anomalies = data_anomalies.drop_duplicates(subset=[reference, segmentation],
                                                    keep=False)

    if data_anomalies.size > 0:
        # Filter so we only have current period stuff
        fix_data = data_anomalies[data_anomalies[time] == int(current_period)][
            [reference, segmentation]]
        fix_data = fix_data.rename(columns={segmentation: stored_segmentation})

        # Now merge these so that the fix_data strata is
        # added as an extra column to the input data
        data = pd.merge(data, fix_data, on=reference, how='left')

        # We should now have a good Strata column in the dataframe - mostly containing
        # null values, containing strata where there was anomoly using an apply method,
        # set strata to be the goodstrata.
        data[segmentation] = data.apply(
            lambda x: x[stored_segmentation]
            if str(x[stored_segmentation]) != 'nan' else x[segmentation], axis=1)
        data = data.drop(stored_segmentation, axis=1)

        # Split on period then merge together so they're same row.
        current_period_anomalies = data_anomalies[
            data_anomalies[time] == int(current_period)].rename(
            columns={segmentation: current_segmentation, time: current_time})

        prev_period_anomalies = data_anomalies[data_anomalies[time]
                                               != int(current_period)].rename(
            columns={segmentation: previous_segmentation, time: previous_time})

        data_anomalies = pd.merge(current_period_anomalies, prev_period_anomalies,
                                  on=reference)

    return data, data_anomalies


def lambda_handler(event, context):
    """
    This wrangler is used to prepare data for the calculate movements statistical method.
    The method requires a column per question to store the movements, named as follows:
    'movement_questionNameAndNumber'. The wrangler checks for non response and if everyone
    has responded the calculate movements is skipped.
    :param event: N/A
    :param context: N/A
    :return: Success - True/False & Checkpoint
    """
    to_be_imputed = True
    current_module = "Imputation Movement - Wrangler."
    logger = logging.getLogger("Starting " + current_module)
    error_message = ''
    log_message = ''
    checkpoint = 0

    try:

        logger.info("Starting " + current_module)

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")

        # Set up clients
        sqs = boto3.client('sqs', region_name='eu-west-2')
        lambda_client = boto3.client('lambda', region_name="eu-west-2")
        logger.info("Setting-up environment configs")

        # Event vars
        calculation_type = event['RuntimeVariables']["calculation_type"]
        distinct_values = event['RuntimeVariables']["distinct_values"].split(",")

        checkpoint = config['checkpoint']
        bucket_name = config['bucket_name']
        in_file_name = config["in_file_name"]
        incoming_message_group = config['incoming_message_group']
        method_name = config['method_name']
        non_response_file = config['non_response_file']
        out_file_name = config["out_file_name"]
        period = event['RuntimeVariables']['period']
        questions_list = config['questions_list']
        previous_period_file = config['previous_period_file']
        response_type = config['response_type']  # Set as "response_type"
        sns_topic_arn = config['sns_topic_arn']
        sqs_message_group_id = config['sqs_message_group_id']
        sqs_queue_url = config['sqs_queue_url']
        time = config['time']  # Set as "period"

        # Import for strata miss-match
        current_segmentation = config['current_segmentation']  # Set as "current_strata"
        current_time = config['current_time']  # Set as "current_period"
        previous_segmentation = config['previous_segmentation']  # Set "previous_strata"
        previous_time = config['previous_time']  # Set as "previous_period"
        reference = config['reference']  # Set as "responder_id"
        segmentation = config['segmentation']  # Set as "strata"
        stored_segmentation = config['stored_segmentation']  # Set as "goodstrata"

        previous_period_data = funk.read_dataframe_from_s3(bucket_name,
                                                           previous_period_file)
        logger.info("Completed reading data from s3")

        data, receipt_handler = funk.get_dataframe(sqs_queue_url, bucket_name,
                                                   in_file_name,
                                                   incoming_message_group)
        logger.info("Successfully retrieved data")
        # Create a Dataframe where the response column
        # value is set as 1 i.e non responders
        filtered_non_responders = data.loc[(data[response_type] == 1) &
                                           (data[time] == int(period))]

        logger.info("Successfully created filtered non responders DataFrame")

        response_check = len(filtered_non_responders.index)

        # If greater than 0 it means there is non-responders so Imputation need to be run
        if response_check > 0:

            non_responders = data[data[response_type] == 1]
            non_responders_json = non_responders.to_json(orient='records')

            logger.info("Successfully created non-responders json")

            funk.save_to_s3(bucket_name, non_response_file, non_responders_json)

            logger.info("Successfully saved to s3 bucket")

            # Ensure that only responder_ids with a response
            # type of 2 (returned) get picked up
            data = data[data[response_type] == 2]

            # Merged together so it can be sent via the payload to the method
            merged_data = pd.concat([data, previous_period_data])

            logger.info("Successfully filtered and merged the previous period data")

            anomalies = []

            # Pass to mismatch detector to look for and fix strata mismatches
            if "strata" in distinct_values:
                merged_data, anomalies = strata_mismatch_detector(
                    merged_data,
                    period, time,
                    reference, segmentation,
                    stored_segmentation,
                    current_time,
                    previous_time,
                    current_segmentation,
                    previous_segmentation)

            logger.info("Successfully completed strata mismatch detection")

            for question in questions_list.split(','):
                merged_data['movement_' + question] = 0.0

            json_ordered_data = merged_data.to_json(orient='records')

            json_payload = {
                "json_data": json_ordered_data,
                "calculation_type": calculation_type,
                "questions_list": questions_list
            }

            logger.info("Successfully created movement columns on the data")

            imputed_data = lambda_client.invoke(FunctionName=method_name,
                                                Payload=json.dumps(json_payload))

            logger.info("Successfully invoked the movement method lambda")

            json_response = json.loads(imputed_data.get('Payload').read().decode("UTF-8"))

            imputation_run_type = "Calculate Movement."
            funk.save_data(bucket_name, out_file_name,
                           json_response, sqs_queue_url, sqs_message_group_id)

            logger.info("Successfully sent the data to s3")

        else:

            to_be_imputed = False
            imputation_run_type = "Has Not Run."
            anomalies = pd.DataFrame
            funk.save_data(
                bucket_name,
                in_file_name,
                data.to_json(orient="records"),
                sqs_queue_url,
                sqs_message_group_id
            )

            logger.info("Successfully sent the unchanged data to s3")

        if receipt_handler:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=receipt_handler)

        funk.send_sns_message_with_anomalies(checkpoint,
                                             str(anomalies), sns_topic_arn,
                                             'Imputation - ' + imputation_run_type)

        logger.info("Successfully sent the SNS message")

    except AttributeError as e:
        error_message = "Bad data encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = "Parameter validation error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ClientError as e:
        error_message = "AWS Error (" \
                        + str(e.response['Error']['Code']) + ") " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = "Key Error in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except IncompleteReadError as e:
        error_message = "Incomplete Lambda response encountered in " \
                        + current_module + " |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except Exception as e:
        error_message = "General Error in " \
                        + current_module + " (" \
                        + str(type(e)) + ") |- " \
                        + str(e.args) + " | Request ID: " \
                        + str(context.aws_request_id)

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:

        if(len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}
        else:
            logger.info("Successfully completed module: " + current_module)
            return {
                "success": True,
                "checkpoint": checkpoint,
                "impute": to_be_imputed,
                "distinct_values": distinct_values
            }
